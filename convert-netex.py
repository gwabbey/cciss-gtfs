#!/usr/bin/env python3
"""
usage:
    python convert-netex.py --json netex.json --out output-dir

JSON format:
    [
      {
        "name": "Trenitalia",
        "url": "https://example.com/netex.xml.gz",
        "output": "trenitalia.gtfs.zip"
      }
    ]
"""
import argparse
import csv
import gzip
import json
import logging
import re
import requests
from collections import defaultdict
from lxml import etree
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile, ZIP_DEFLATED

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger("netex2gtfs")

NS = {"n": "http://www.netex.org.uk/netex", "gml": "http://www.opengis.net/gml/3.2"}


def ns(tag):
    return f"{{{NS['n']}}}{tag}"


from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def download(url: str, dest: Path):
    LOG.info(f"Downloading {url}")

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"],
                    raise_on_status=False, )
    session.mount('https://', HTTPAdapter(max_retries=retries))

    for attempt in range(1, 4):
        try:
            with session.get(url, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            LOG.info(f"Downloaded {dest.stat().st_size / 1e6:.1f} MB")
            return
        except Exception as e:
            LOG.warning(f"Attempt {attempt}/3 failed for {url}: {e}")
            if attempt == 3:
                raise
            import time
            time.sleep(5 * attempt)


def _infer_day_offsets(times):
    if not times:
        return times

    result = []
    last_day = 0
    last_seconds = -1

    for sp_ref_id, arr, dep, arr_offset, dep_offset in times:
        arr_seconds = parse_time_to_seconds(arr) if arr else -1
        dep_seconds = parse_time_to_seconds(dep) if dep else -1

        if arr_offset is not None:
            final_arr_offset = arr_offset
            last_day = arr_offset
            last_seconds = arr_seconds
        elif arr_seconds >= 0:
            if last_seconds >= 0:
                if arr_seconds < last_seconds and last_seconds - arr_seconds > 10800:
                    last_day += 1
            final_arr_offset = last_day
            last_seconds = arr_seconds
        else:
            final_arr_offset = last_day

        if dep_offset is not None:
            final_dep_offset = dep_offset
            last_day = dep_offset
            last_seconds = dep_seconds
        elif dep_seconds >= 0:
            if arr_seconds >= 0:
                if dep_seconds < arr_seconds and arr_seconds - dep_seconds > 10800:
                    final_dep_offset = final_arr_offset + 1
                    last_day = final_dep_offset
                    last_seconds = dep_seconds
                else:
                    final_dep_offset = final_arr_offset
                    last_seconds = dep_seconds
            else:
                if last_seconds >= 0 and dep_seconds < last_seconds and last_seconds - dep_seconds > 10800:
                    last_day += 1
                final_dep_offset = last_day
                last_seconds = dep_seconds
        else:
            final_dep_offset = last_day

        result.append((sp_ref_id, arr, dep, final_arr_offset, final_dep_offset))

    return result


class NeTExExtractor:
    WEEKDAYS = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6}

    def __init__(self, parse_service_links=True):
        self.parse_service_links = parse_service_links

        self.stops = {}  # stop_id -> (name, lat, lon)
        self.lines = {}  # line_id -> (short, long, type)
        self.operators = {}  # operator_id -> (name, url, phone)
        self.authorities = {}  # authority_id -> (name, url, phone)

        self.patterns = {}  # pattern_id -> [(order, stop_id, service_link_ref), ...]
        self.pattern_line = {}  # pattern_id -> line_id
        self.stop_in_pattern = {}  # StopPointInJourneyPattern_id -> (pattern_id, order, stop_id)

        self.service_links = {}  # link_id -> [(lon, lat), ...]

        self.journeys = []  # (journey_id, pattern_id, headsign, daytype_id, journey_name)
        self.journey_times_raw = defaultdict(list)  # journey_id -> [(sp_ref, arr, dep, arr_offset, dep_offset)]

        self.calendar_start = None
        self.calendar_end = None
        self.day_types = {}

        self.day_type_assignments = defaultdict(list)  # day_type_id -> [operating_period_id, ...]
        self.operating_periods = {}  # period_id -> (from_date, to_date, valid_day_bits)

    def extract(self, xml_path: Path):
        LOG.info("Parsing NeTEx (streaming)...")

        targets = {ns("ScheduledStopPoint"), ns("Line"), ns("Operator"), ns("Authority"), ns("ServiceJourneyPattern"),
                   ns("ServiceJourney"), ns("ServiceCalendar"), ns("DayTypeAssignment"), ns("UicOperatingPeriod"),
                   ns("OperatingPeriod"), }

        if self.parse_service_links:
            targets.add(ns("ServiceLink"))

        ctx = etree.iterparse(str(xml_path), events=("end",), tag=targets, huge_tree=True, recover=True)

        count = 0
        for _, elem in ctx:
            count += 1
            tag = etree.QName(elem).localname

            if tag == "ScheduledStopPoint":
                self._parse_stop(elem)
            elif tag == "Line":
                self._parse_line(elem)
            elif tag == "Operator":
                self._parse_operator(elem)
            elif tag == "Authority":
                self._parse_authority(elem)
            elif tag == "ServiceJourneyPattern":
                self._parse_pattern(elem)
            elif tag == "ServiceJourney":
                self._parse_journey(elem)
            elif tag == "ServiceCalendar":
                self._parse_service_calendar(elem)
            elif tag == "ServiceLink" and self.parse_service_links:
                self._parse_service_link(elem)
            elif tag == "DayTypeAssignment":
                self._parse_day_type_assignment(elem)
            elif tag in ("UicOperatingPeriod", "OperatingPeriod"):
                self._parse_operating_period(elem)

            elem.clear()

            if count % 1000 == 0:
                parent = elem.getparent()
                if parent is not None:
                    while elem.getprevious() is not None:
                        del parent[0]

            if count % 100000 == 0:
                LOG.info(f"Processed {count:,} elements...")

        LOG.info(f"Parsed {count:,} elements total")

    def _text(self, elem, xpath):
        node = elem.find(xpath, NS)
        return node.text.strip() if node is not None and node.text else None

    def _parse_stop(self, elem):
        sid = elem.get("id")
        name = self._text(elem, ".//n:Name") or self._text(elem, ".//n:ShortName") or sid
        lat = self._text(elem, ".//n:Location/n:Latitude")
        lon = self._text(elem, ".//n:Location/n:Longitude")
        if lat and lon:
            self.stops[sid] = (name, lat, lon)

    def _parse_line(self, elem):
        lid = elem.get("id")
        short = self._text(elem, ".//n:ShortName") or ""
        long_name = self._text(elem, ".//n:Name") or short
        mode = self._text(elem, ".//n:TransportMode") or "bus"
        rtype = {"bus": 3, "tram": 0, "metro": 1, "rail": 2, "ferry": 4}.get(mode.lower(), 3)
        self.lines[lid] = (short, long_name, rtype)

    def _parse_operator(self, elem):
        oid = elem.get("id")
        name = self._text(elem, ".//n:Name") or self._text(elem, ".//n:ShortName") or oid
        url = self._text(elem, ".//n:ContactDetails/n:Url") or ""
        phone = self._text(elem, ".//n:ContactDetails/n:Phone") or ""
        self.operators[oid] = (name, url, phone)

    def _parse_authority(self, elem):
        aid = elem.get("id")
        name = self._text(elem, ".//n:Name") or self._text(elem, ".//n:ShortName") or aid
        url = self._text(elem, ".//n:ContactDetails/n:Url") or ""
        phone = self._text(elem, ".//n:ContactDetails/n:Phone") or ""
        self.authorities[aid] = (name, url, phone)

    def _parse_pattern(self, elem):
        pid = elem.get("id")

        line_ref = elem.find(".//n:RouteView/n:LineRef", NS)
        if line_ref is not None:
            self.pattern_line[pid] = line_ref.get("ref")

        stops = []
        for sp in elem.findall(".//n:pointsInSequence/n:StopPointInJourneyPattern", NS):
            sp_id = sp.get("id")
            order = int(sp.get("order", 0))

            ref = sp.find("n:ScheduledStopPointRef", NS)
            stop_id = ref.get("ref") if ref is not None else None

            link_ref = sp.find("n:OnwardServiceLinkRef", NS)
            link_id = link_ref.get("ref") if link_ref is not None else None

            if stop_id:
                stops.append((order, stop_id, link_id))
                self.stop_in_pattern[sp_id] = (pid, order, stop_id)

        stops.sort(key=lambda x: x[0])
        self.patterns[pid] = stops

    def _parse_journey(self, elem):
        jid = elem.get("id")

        journey_name = self._text(elem, ".//n:Name") or ""

        pat_ref = elem.find(".//n:JourneyPatternRef", NS)
        if pat_ref is None:
            pat_ref = elem.find(".//n:ServiceJourneyPatternRef", NS)
        pid = pat_ref.get("ref") if pat_ref is not None else None

        headsign = self._text(elem, ".//n:DestinationDisplayRef") or ""
        if not headsign:
            headsign = self._text(elem, ".//n:DestinationDisplay/n:FrontText") or ""

        dt_ref = elem.find(".//n:dayTypes/n:DayTypeRef", NS)
        daytype_id = dt_ref.get("ref") if dt_ref is not None else None

        times = []
        for pt in elem.findall(".//n:passingTimes/n:TimetabledPassingTime", NS):
            sp_ref = pt.find("n:StopPointInJourneyPatternRef", NS)
            if sp_ref is None:
                continue

            sp_ref_id = sp_ref.get("ref")
            arr = self._text(pt, "n:ArrivalTime") or ""
            dep = self._text(pt, "n:DepartureTime") or ""

            arr_offset_str = self._text(pt, "n:ArrivalDayOffset")
            dep_offset_str = self._text(pt, "n:DepartureDayOffset")

            arr_offset = int(arr_offset_str) if arr_offset_str else None
            dep_offset = int(dep_offset_str) if dep_offset_str else None

            times.append((sp_ref_id, arr, dep, arr_offset, dep_offset))

        times = _infer_day_offsets(times)

        self.journey_times_raw[jid] = times
        self.journeys.append((jid, pid, headsign, daytype_id, journey_name))

    def _parse_service_link(self, elem):
        lid = elem.get("id")

        pos_list = elem.find(".//gml:posList", NS)
        if pos_list is None or not pos_list.text:
            return

        coords = []
        values = pos_list.text.strip().split()
        for i in range(0, len(values) - 1, 2):
            try:
                lon, lat = float(values[i]), float(values[i + 1])
                coords.append((lat, lon))
            except ValueError:
                continue

        if coords:
            self.service_links[lid] = coords

    def _parse_service_calendar(self, elem):
        from_date = self._text(elem, ".//n:FromDate")
        to_date = self._text(elem, ".//n:ToDate")
        if from_date:
            self.calendar_start = self._clean_date(from_date)
        if to_date:
            self.calendar_end = self._clean_date(to_date)

    def _clean_date(self, date_str: str) -> str:
        match = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
        if match:
            return f"{match.group(1)}{match.group(2)}{match.group(3)}"
        return date_str.replace("-", "")[:8]

    def _parse_day_type_assignment(self, elem):
        """Link DayType to OperatingPeriod."""
        dt_ref = elem.find(".//n:DayTypeRef", NS)
        op_ref = elem.find(".//n:OperatingPeriodRef", NS)

        if dt_ref is None:
            return

        dtid = dt_ref.get("ref")

        if op_ref is not None:
            self.day_type_assignments[dtid].append(op_ref.get("ref"))
        else:
            date = self._text(elem, ".//n:Date")
            if date:
                clean = self._clean_date(date)
                self.day_type_assignments[dtid].append(("date", clean))

    def _parse_operating_period(self, elem):
        pid = elem.get("id")

        from_date = self._text(elem, ".//n:FromDate")
        to_date = self._text(elem, ".//n:ToDate")
        bits = self._text(elem, ".//n:ValidDayBits")

        if from_date:
            from_clean = self._clean_date(from_date)
            to_clean = self._clean_date(to_date) if to_date else from_clean
            self.operating_periods[pid] = (from_clean, to_clean, bits or "")


class GTFSWriter:
    HEADERS = {"agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_phone"],
               "stops.txt": ["stop_id", "stop_name", "stop_lat", "stop_lon"],
               "routes.txt": ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
               "trips.txt": ["route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name"],
               "stop_times.txt": ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
               "calendar.txt": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
                                "sunday", "start_date", "end_date"],
               "calendar_dates.txt": ["service_id", "date", "exception_type"]}

    def __init__(self, out: Path):
        self.out = out
        self.out.mkdir(parents=True, exist_ok=True)
        self.files = {}
        self.writers = {}

        for fname, headers in self.HEADERS.items():
            f = open(out / fname, "w", newline="", encoding="utf-8")
            w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            w.writeheader()
            self.files[fname] = f
            self.writers[fname] = w

    def write(self, fname: str, row: dict):
        self.writers[fname].writerow(row)

    def close(self):
        for f in self.files.values():
            f.close()

    def package(self, zip_path: Path):
        LOG.info(f"Creating {zip_path}")
        with ZipFile(zip_path, "w", ZIP_DEFLATED) as z:
            for fname in self.HEADERS:
                fpath = self.out / fname
                if fpath.exists() and fpath.stat().st_size > len(self.HEADERS[fname][0]):
                    z.write(fpath, fname)


def parse_time_to_seconds(t: str) -> int:
    if not t:
        return -1
    try:
        parts = t.strip().split(":")
        if len(parts) >= 2:
            h, m = int(parts[0]), int(parts[1])
            s = int(parts[2]) if len(parts) > 2 else 0
            return h * 3600 + m * 60 + s
    except (ValueError, IndexError):
        pass
    return -1


def seconds_to_gtfs_time(secs: int) -> str:
    if secs < 0:
        return ""
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def apply_day_offset(time_str: str, offset: int) -> str:
    secs = parse_time_to_seconds(time_str)
    if secs < 0:
        return ""
    secs += offset * 24 * 3600
    return seconds_to_gtfs_time(secs)


from datetime import datetime, timedelta


def parse_date(d: str) -> datetime:
    return datetime.strptime(d, "%Y%m%d")


def format_date(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def expand_valid_day_bits(from_date: str, bits: str) -> list:
    dates = []
    start = parse_date(from_date)
    for i, bit in enumerate(bits):
        if bit == "1":
            dates.append(format_date(start + timedelta(days=i)))
    return dates


def convert(extractor: NeTExExtractor, writer: GTFSWriter):
    LOG.info("Writing GTFS...")

    agency_written = False
    agency_id = 1

    for oid, (name, url, phone) in extractor.operators.items():
        writer.write("agency.txt",
                     {"agency_id": str(agency_id), "agency_name": name, "agency_url": url or "https://example.com",
                      "agency_timezone": "Europe/Rome", "agency_phone": phone})
        agency_id += 1
        agency_written = True

    if not agency_written:
        for aid, (name, url, phone) in extractor.authorities.items():
            writer.write("agency.txt",
                         {"agency_id": str(agency_id), "agency_name": name, "agency_url": url or "https://example.com",
                          "agency_timezone": "Europe/Rome", "agency_phone": phone})
            agency_id += 1
            agency_written = True

    if not agency_written:
        writer.write("agency.txt",
                     {"agency_id": "1", "agency_name": "Transit Agency", "agency_url": "https://example.com",
                      "agency_timezone": "Europe/Rome", "agency_phone": ""})

    for sid, (name, lat, lon) in extractor.stops.items():
        writer.write("stops.txt", {"stop_id": sid, "stop_name": name, "stop_lat": lat, "stop_lon": lon})

    written_routes = set()
    for lid, (short, long_name, rtype) in extractor.lines.items():
        writer.write("routes.txt",
                     {"route_id": lid, "agency_id": "1", "route_short_name": short, "route_long_name": long_name,
                      "route_type": rtype})
        written_routes.add(lid)

    LOG.info("Processing calendar (UicOperatingPeriod only)...")

    services_written = set()

    for dtid, assignments in extractor.day_type_assignments.items():
        all_dates = []

        for assignment in assignments:
            if isinstance(assignment, tuple) and assignment[0] == "date":
                all_dates.append(assignment[1])
            elif assignment in extractor.operating_periods:
                from_date, to_date, bits = extractor.operating_periods[assignment]
                if bits:
                    all_dates.extend(expand_valid_day_bits(from_date, bits))

        if all_dates:
            for date in all_dates:
                writer.write("calendar_dates.txt", {"service_id": dtid, "date": date, "exception_type": 1})
            services_written.add(dtid)

    if not services_written:
        default_start = extractor.calendar_start or "20240101"
        default_end = extractor.calendar_end or "20251231"
        start_dt = parse_date(default_start)
        end_dt = parse_date(default_end)

        current = start_dt
        while current <= end_dt:
            writer.write("calendar_dates.txt",
                         {"service_id": "default", "date": format_date(current), "exception_type": 1})
            current += timedelta(days=1)
        services_written.add("default")

    LOG.info(f"Calendar: {len(services_written)} services using calendar_dates.txt")

    trips_written = 0
    stop_times_written = 0

    for jid, pid, headsign, daytype_id, journey_name in extractor.journeys:
        route_id = extractor.pattern_line.get(pid)
        if not route_id or route_id not in written_routes:
            continue

        service_id = daytype_id if daytype_id in services_written else "default"
        trip_short_name = ""

        if "TRENORD" in jid and journey_name:
            trip_short_name = journey_name
        else:
            match = re.search(r':\d+_\d+_\d+-([A-Z0-9]+)-', jid)
            if match:
                trip_short_name = match.group(1)
            else:
                match = re.search(r'[-_]([A-Z0-9]+\d+)[-_]', jid)
                if match:
                    trip_short_name = match.group(1)

        writer.write("trips.txt",
                     {"route_id": route_id, "service_id": service_id, "trip_id": jid, "trip_headsign": headsign,
                      "trip_short_name": trip_short_name})
        trips_written += 1

        raw_times = extractor.journey_times_raw.get(jid, [])
        resolved = []
        for sp_ref_id, arr, dep, arr_off, dep_off in raw_times:
            if sp_ref_id in extractor.stop_in_pattern:
                _, order, stop_id = extractor.stop_in_pattern[sp_ref_id]
                arr_gtfs = apply_day_offset(arr, arr_off) if arr else ""
                dep_gtfs = apply_day_offset(dep, dep_off) if dep else ""
                resolved.append((order, stop_id, arr_gtfs, dep_gtfs))

        resolved.sort(key=lambda x: x[0])

        for order, stop_id, arr, dep in resolved:
            if stop_id in extractor.stops:
                writer.write("stop_times.txt",
                             {"trip_id": jid, "arrival_time": arr or dep, "departure_time": dep or arr,
                              "stop_id": stop_id, "stop_sequence": order})
                stop_times_written += 1

    LOG.info(f"Written: {trips_written} trips, {stop_times_written} stop_times")


def main():
    p = argparse.ArgumentParser(description="Fast Italian NeTEx to GTFS converter")
    p.add_argument("--json", required=True, help="JSON file with url + output entries")
    p.add_argument("--out", required=True, help="Output directory for GTFS zips")
    args = p.parse_args()

    json_path = Path(args.json)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(json_path, "r", encoding="utf-8") as f:
        entries = json.load(f)

    for entry in entries:
        url = entry.get("url")
        output_filename = entry.get("output")

        if not url or not output_filename:
            LOG.error(f"Skipping invalid entry: {entry}")
            continue

        LOG.info(f"--- Processing {entry.get('name', 'Unknown')} ---")
        try:
            convert_single(url, out_dir / output_filename)
        except Exception as e:
            LOG.error(f"Failed to convert {output_filename}: {e}", exc_info=True)
            raise


def convert_single(url: str, out_path: Path):
    with TemporaryDirectory(prefix="netex_") as tmpdir:
        tmpdir = Path(tmpdir)
        gz_path = tmpdir / "input.xml.gz"

        download(url, gz_path)

        try:
            with gzip.open(gz_path, 'rb') as f:
                f.peek(1)
        except (EOFError, OSError, gzip.BadGzipFile):
            LOG.error(f"Downloaded file {gz_path} is corrupted or incomplete. Retrying.")
            raise ValueError("Corrupted download")

        xml_path = tmpdir / "input.xml"
        LOG.info("Decompressing...")
        with gzip.open(gz_path, "rb") as fin, open(xml_path, "wb") as fout:
            try:
                while chunk := fin.read(1024 * 1024):
                    fout.write(chunk)
            except EOFError:
                LOG.error("Gzip stream cut off during decompression.")
                raise ValueError("Incomplete Gzip stream")

        extractor = NeTExExtractor()
        extractor.extract(xml_path)

        gtfs_dir = tmpdir / "gtfs"
        writer = GTFSWriter(gtfs_dir)

        convert(extractor, writer)
        writer.close()

        writer.package(out_path)


if __name__ == "__main__":
    main()
