# BODS's timetable data and the 2026 holidays

Checked 26 November 2026 against BODS's south-east GTFS, `feed_version
20261124_044012`, and the operators' own TransXChange in BODS's bulk archive
(`https://data.bus-data.dft.gov.uk/timetable/download/bulk_archive`, the
archive of 26 November). This site corrects both faults in its weekly timetable
build (`scripts/timetable_corrections.py`, entries and caveats in
`data/timetable_corrections.json`), and a board on an affected day says so.

## 1. New Year's Eve arrives as 1 December

**What the operators publish.** Brighton & Hove's route 7 file for 29 November
to 20 December, and Stagecoach's two Coastliner 700 files (`PK0002571-53` V31
and `PK0002571-116` V1), never mention 1 December. Every profile that does not
run on holidays lists the same 13: Christmas Day, Boxing Day, Good Friday, New
Year's Day, the late summer, May and spring holidays, Easter Monday, the three
substitute days, Christmas Eve and New Year's Eve. Brighton & Hove's own
website shows route 7 running its normal Tuesday timetable on 1 December.

**What BODS's GTFS says.**

- Brighton & Hove route 7, Monday to Friday, 29 November to 20 December:
  calendar `18949` removes one date, 1 December. No holiday falls in that
  window.
- The 700, Monday to Friday (calendar `83`, 184 journeys, the same 184 as the
  V1 file): it removes every one of its holidays, except that 31 December is
  missing and 1 December is there instead. Its Sunday calendar (`806`, 130 +
  122 journeys) does the same.
- Across the region: of the 313 calendars covering December that skip
  Christmas Eve, 268 also skip 1 December, and only 11 skip 31 December outside
  a school-holiday run.
- The fault works both ways. Services that run *only* on New Year's Eve are
  added on 1 December. A Thursday-only calendar (First Essex 703) and a
  Saturday-only one are added on Tuesday 1 December, and calendars that run
  only on holidays are added on 1 December alongside them, which is why some
  operators show more trips on 1 December than on a normal Tuesday.
- Services dated day by day (school days) have 1 December missing from
  otherwise unbroken runs of weekdays.

**What it does.**

- On 1 December 2026, 30 of the region's 49 larger operators lose over half
  their trips against Tuesday 8 December. Brighton & Hove and the 700 lose
  every one.
- On 31 December, journeys the operators exclude on New Year's Eve stay in,
  while their New Year's Eve services sit on 1 December instead.

**Not this.** The earlier explanation, that St Andrew's Day's 2025 substitute
date was carried over, does not fit. The files above contain no Scottish
holiday at all. And it would not explain 31 December going missing, or
Thursday and Saturday services being added on a Tuesday.

## 2. Christmas Day and New Year's Day services that never run

Brighton & Hove's route 7 files define both services:

- a Christmas Day service: `…20261220_20261226_2464816.xml`, Friday, running
  on Christmas Day;
- a New Year's Day service: `…20261231_20270102_2465233.xml`, Monday and
  Friday, running on New Year's Day.

BODS's calendars for them (`16612` and `18970`) remove 25 December and 1
January, the only days each can run, so the services vanish. Brighton & Hove's
website lists route 7 on both days (144 and 808 times).

Each profile also lists `ChristmasDayHoliday` or `NewYearsDayHoliday` (the
substitute days) among the days it does not run. Neither falls on a weekend in
2026–27, so neither should be a date at all. It looks as if they are being
turned into the day itself. That is our reading, not shown.

## 3. Caveats

- Shown in full for Brighton & Hove route 7 and the Coastliner 700. Other
  operators' files were not all read. The pattern across the feed fits, but is
  not proof for each one.
- The counts are of scheduled trips in one region's file (south east), not of
  buses or passengers. Other regions were not checked.
- What each operator actually runs on New Year's Eve evening is not
  established. Our correction restores what their files say.
- Our correction leaves a 1 December exception alone where it sits in a run of
  dates. A school genuinely closed on 1 December and dated day by day would
  wrongly get the day back.

## 4. What would show this wrong

An operator saying it runs no service, or a reduced one, on 1 December 2026, or
that its regular timetable runs on New Year's Eve. Or a TransXChange file that
names 1 December as a day of non-operation.

## 5. Reproduce

```sh
curl -L -o south_east.zip https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/south_east/
python docs/reliability/bods_holiday_check.py south_east.zip
```

The script is `docs/reliability/bods_holiday_check.py` in this repository. Its
output on feed `20261124_044012` gives the figures in section 1.
