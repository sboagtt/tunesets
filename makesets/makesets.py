import os
import sys
from urllib.request import Request, urlopen, URLError
from collections import namedtuple
from recordtype import recordtype
from typing import List, Dict
# import the Beautiful soup functions to parse the data returned from the website
from bs4 import BeautifulSoup
from enum import Enum
import time


import pickle
import re

tune_list_pickle_file_name = 'tune_list_sets.pkl'

sys.setrecursionlimit(10000)

MAX_TUNES_IN_SET = 3

TuneInSetSpec = recordtype('TuneInSetSpec', 'tune_id tune_name from_album')

TuneFollowsGoesIntoSpec = namedtuple('TuneFollowsGoesIntoSpec', 'tune_id tune_name follows goes_into')

TuneFollowsGoesIntoSpecAnalysis = recordtype('TuneFollowsGoesIntoSpecAnalysis',
                                             '''tune_id 
                                             tune_name 
                                             follows 
                                             goes_into
                                             ''')

TuneSet = recordtype('TuneSet', 'tune_list locked from_album')

regex = r"#(?P<tune_id>[0-9]+)\[(?P<tune_name>[A-z0-9 \']+).*\]"


class FollowsOrGoesInto(Enum):
    FOLLOWS = 1
    GOES_INTO = 2


def extract_tunes_from_set_table(soup, table_id: str) -> List[TuneInSetSpec]:
    follows_list: List[TuneInSetSpec] = []

    follows = soup.find_all("table", id=table_id)

    for follow_table in follows:
        rows = follow_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) > 0:
                tune_anchors = cells[0].find_all("a")
                if len(tune_anchors) == 1:
                    title = tune_anchors[0].string
                    href = tune_anchors[0].get('href')
                    from_album = cells[1].string
                elif len(tune_anchors) > 0:
                    title = tune_anchors[1].string
                    href = tune_anchors[1].get('href')
                    from_album = cells[1].string
                if href is not None:
                    _, _, tune_id, _ = href.split("/")
                    record = TuneInSetSpec(tune_id=tune_id, tune_name=title, from_album=from_album)
                    follows_list.append(record)
                else:
                    print("ERROR! Did not find anchor cell!")
                    print(row.prettify())
                    sys.exit(-1)

    return follows_list


def process_set_link_list(tune_analysis: TuneFollowsGoesIntoSpecAnalysis,
                          tune_dict: Dict[str, TuneFollowsGoesIntoSpecAnalysis],
                          set_list: List[TuneSet], set_id_top: int,
                          siblings: List[TuneInSetSpec], which: FollowsOrGoesInto,
                          match_albums: bool = True):

    for sibling in siblings:
        if sibling.tune_id in tune_dict:
            for set_id2, tunes_set in enumerate(set_list):
                if tunes_set is None:
                    continue
                if tunes_set.locked:
                    continue

                # Constrain sets to be from same album
                if match_albums and tunes_set.from_album is not None:
                    if tunes_set.from_album != sibling.from_album:
                        continue

                tune_list = tunes_set.tune_list
                if len(tune_list) < MAX_TUNES_IN_SET:
                    if which == FollowsOrGoesInto.FOLLOWS:
                        next_sibling_set_rec = tune_list[len(tune_list) - 1]
                    else:
                        next_sibling_set_rec = tune_list[0]

                    if next_sibling_set_rec.tune_id == sibling.tune_id:
                        tune_record = TuneInSetSpec(tune_id=tune_analysis.tune_id,
                                                    tune_name=tune_analysis.tune_name,
                                                    from_album=sibling.from_album)
                        if len(tune_list) == 1:
                            tune_list[0].from_album = sibling.from_album
                        if which == FollowsOrGoesInto.FOLLOWS:
                            tune_list.append(tune_record)
                        else:
                            tune_list.insert(0, tune_record)

                        if tunes_set.from_album is None:
                            tunes_set.from_album = sibling.from_album

                        # print("moved to be a follow tune: %d to %d, len is now %d" % (
                        #     set_id_top, set_id2, len(set_list[set_id2])))
                        set_list[set_id_top] = None
                        break
        if set_list[set_id_top] is None:
            # Then we've already processed it
            break


def find_index_for_single_tune_in_set_list(set_list: List[TuneSet], tune_id: str):
    for set_id, tunes_set in enumerate(set_list):
        if tunes_set is not None:
            if tunes_set.tune_list[0].tune_id == tune_id:
                return set_id
    return -1


def process_goes_into(set_list: List[TuneSet],
                      prev_id: str,
                      goes_into_id: str,
                      goes_into_name: str,
                      goes_into_album: str):
    for set_id2, tunes_set in enumerate(set_list):
        if tunes_set is None:
            continue
        tune_list = tunes_set.tune_list
        if len(tune_list) < MAX_TUNES_IN_SET:
            next_sibling_set_rec = tune_list[len(tune_list) - 1]

            if next_sibling_set_rec.tune_id == prev_id:
                tune_record = TuneInSetSpec(tune_id=goes_into_id,
                                            tune_name=goes_into_name,
                                            from_album=goes_into_album)
                if len(tune_list) == 1:
                    tunes_set.locked = True
                    tunes_set.from_album = goes_into_album
                    tune_list[0].from_album = goes_into_album

                tune_list.append(tune_record)

                # print("moved to be a follow tune: %d to %d, len is now %d" % (
                #     set_id_top, set_id2, len(set_list[set_id2])))
                set_id_top = find_index_for_single_tune_in_set_list(set_list, goes_into_id)
                set_list[set_id_top] = None
                break


def clean_nulls_from_list(items):
    return [e for e in items if e is not None]


def main():
    print(os.getcwd())

    tune_list: List[TuneFollowsGoesIntoSpec] = []
    tune_dict: Dict[str, TuneFollowsGoesIntoSpecAnalysis] = {}

    if os.path.isfile(tune_list_pickle_file_name):
        with open(tune_list_pickle_file_name, 'rb') as f:
            tune_list = pickle.load(f)
    else:
        last_throttle_time = time.time()
        with open("data/playlist.txt") as f:
            for line in f:
                duration_since_last_throttle = time.time() - last_throttle_time
                # print("duration_since_last_throttle: %f" % duration_since_last_throttle)
                if duration_since_last_throttle > 2.0:
                    print("resting...", flush=True)
                    time.sleep(1.0)
                    last_throttle_time = time.time()

                line = line.strip()
                rhythm, title, structure, key, first_2_bars, tags, tune_id = line.split("\t")

                page_url = 'https://www.irishtune.info/tune/%s/' % tune_id

                print(page_url)

                req = Request(page_url, headers={'User-Agent': 'Mozilla/5.0'})

                for x in range(0, 4):
                    try:
                        web_byte = urlopen(req).read()
                        break
                    except URLError as e:
                        print('URLError = ' + str(e.reason), flush=True)
                        time.sleep(2.0)

                page = web_byte.decode('utf-8')

                soup = BeautifulSoup(page, features="html.parser")

                follows_list: List[TuneInSetSpec] = extract_tunes_from_set_table(soup, "follows")
                goes_into_list: List[TuneInSetSpec] = extract_tunes_from_set_table(soup, "goesInto")

                tune_record = TuneFollowsGoesIntoSpec(tune_id=tune_id, tune_name=title,
                                                      follows=follows_list, goes_into=goes_into_list)
                tune_list.append(tune_record)

    with open('tune_list_sets.pkl', 'wb') as f:
        pickle.dump(tune_list, f)

    for tune_rec in tune_list:
        tune_dict[tune_rec.tune_id] = TuneFollowsGoesIntoSpecAnalysis(tune_id=tune_rec.tune_id,
                                                                      tune_name=tune_rec.tune_name,
                                                                      follows=tune_rec.follows,
                                                                      goes_into=tune_rec.goes_into)
    if False:
        for tune_rec in tune_list:
            print("%s: %s" % (tune_rec.tune_id, tune_rec.tune_name))

            print("    This tune follows:")
            for prev_tune in tune_rec.follows:
                if prev_tune.tune_id in tune_dict:
                    print("        id: %s, name: %s, from_album: %s" % (prev_tune.tune_id,
                                                                        prev_tune.tune_name,
                                                                        prev_tune.from_album))

            print("    This tune goes into:")
            for goes_into_tune in tune_rec.goes_into:
                if goes_into_tune.tune_id in tune_dict:
                    print("        id: %s, name: %s, from_album: %s" % (goes_into_tune.tune_id,
                                                                        goes_into_tune.tune_name,
                                                                        goes_into_tune.from_album))

    set_list: List[TuneSet] = []

    print("=== set attempt ===")
    for key in tune_dict:
        tune_analysis = tune_dict[key]
        set_list.append(TuneSet(tune_list=[TuneInSetSpec(tune_id=tune_analysis.tune_id,
                                                         tune_name=tune_analysis.tune_name,
                                                         from_album=None)],
                                locked=False, from_album=None))

    with open("data/set_overrides.txt") as f:
        for line in f:
            line = line.strip()
            tunes = line.split("/")
            prev_id = None
            for tune in tunes:
                matches = re.match(regex, tune)
                tune_id = matches.group('tune_id')
                tune_name = matches.group('tune_name')
                # print("---> %s %s" % (tune_id, tune_name))

                if prev_id is not None:
                    process_goes_into(set_list, prev_id, tune_id, tune_name, "me")

                prev_id = tune_id

    with open("data/foin_session.txt") as f:
        for line in f:
            line = line.strip()
            tunes = line.split("/")
            prev_id = None
            for tune in tunes:
                matches = re.match(regex, tune)
                tune_id = matches.group('tune_id')
                tune_name = matches.group('tune_name')
                # print("---> %s %s" % (tune_id, tune_name))

                if prev_id is not None:
                    process_goes_into(set_list, prev_id, tune_id, tune_name, "fs")

                prev_id = tune_id

    match_albums = True
    for x in range(0, 20):
        changes_made = 0
        for set_id_top, tune_set in enumerate(set_list):
            if tune_set is None:
                continue
            tune_list = tune_set.tune_list
            if len(tune_list) == 1:
                tune_analysis = tune_dict[tune_list[0].tune_id]

                process_set_link_list(tune_analysis, tune_dict, set_list, set_id_top,
                                      tune_analysis.follows, FollowsOrGoesInto.FOLLOWS, match_albums=match_albums)

                if set_list[set_id_top] is None:
                    # Then we've already processed it, continue to next tune
                    changes_made += 1
                    continue

                process_set_link_list(tune_analysis, tune_dict, set_list, set_id_top,
                                      tune_analysis.goes_into, FollowsOrGoesInto.GOES_INTO, match_albums=match_albums)

                if set_list[set_id_top] is None:
                    # Then we've already processed it, continue to next tune
                    changes_made += 1
                    continue

        set_list = clean_nulls_from_list(set_list)

        if changes_made == 0:
            if match_albums:
                match_albums = False
                print("Trying with match_albums set to false: %d" % x)
            else:
                print("Giving up trying to assemble sets: %d" % x)
                break

    print("=== print set ===")

    for tune_set in set_list:
        sequence_id = 0
        tune_list = tune_set.tune_list
        for tune_spec in tune_list:
            # print("tune_spec type: %s" % type(tune_spec))
            print("#%s[%s, %s]" % (tune_spec.tune_id, tune_spec.tune_name, tune_spec.from_album), end='')
            sequence_id += 1
            if sequence_id < len(tune_list):
                print("/", end='')
            else:
                print(" from_album: %s" % tune_set.from_album)

    with open("data/sets_results.html", 'w') as f:
        f.write("<html>\n")
        for tune_set in set_list:
            sequence_id = 0
            tune_list = tune_set.tune_list
            f.write("<p>")
            for tune_spec in tune_list:
                # print("tune_spec type: %s" % type(tune_spec))
                # print("#%s[%s, %s]" % (tune_spec.tune_id, tune_spec.tune_name, tune_spec.from_album), end='')
                if tune_spec.from_album is None:
                    f.write("<a href=\"https://www.irishtune.info/tune/%s/\">%s</a>" % (tune_spec.tune_id,
                                                                                             tune_spec.tune_name))
                else:
                    f.write("<a href=\"https://www.irishtune.info/tune/%s/\">%s (%s)</a>" % (tune_spec.tune_id,
                                                                                             tune_spec.tune_name,
                                                                                             tune_spec.from_album))

                sequence_id += 1
                if sequence_id < len(tune_list):
                    f.write("/")
                else:
                    pass
                    # print(" from_album: %s" % tune_set.from_album)
            f.write("</p>\n")
        f.write("</html>\n")


main()