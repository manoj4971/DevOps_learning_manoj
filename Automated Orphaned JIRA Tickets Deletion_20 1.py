# Added Epic_Batch to the script

import requests
import json
import sys
from datetime import datetime, timezone
from urllib.parse import quote, unquote
from requests.auth import HTTPBasicAuth
from atr_sdk import ATRConsul, ATRApi
from utils import get_query_body, get_index
from ATRElastic import ATRElastic

# ====== CONFIGURATION ======

EXCLUDE_STATES = ["Closed", "Resolved"]
PER_PAGE = 10000
MAX_EPIC_BATCH_SIZE = 400  # <- tune in as needed
PLUGIN_ENDPOINT = "atr-gateway/ticket-management/api/v1/plugin/plugin-jira/conf"
DELETE_ENDPOINT = "atr-gateway/ticket-management/api/v1/tickets"

# ====== Retrieve ATR Base URL & Token Generation ======
def get_token_and_baseurl():
    atr_consul = ATRConsul()
    admin_password = atr_consul.get('configuration/aaam-atr-v3-identity-management/admin.password')
    raw_base_url = atr_consul.get('configuration/generic/base.url')
    base_url = raw_base_url if raw_base_url.startswith("http") else f"https://{raw_base_url}"
    atr = ATRApi("admin", admin_password, base_url=base_url, toggle_use_whole_base_url=True)
    return atr.token, base_url

# ====== Retrieve plugin configuration via HTTP ======
def retrieve_plugin_configuration(token, base_url):
    url = f"{base_url}/{PLUGIN_ENDPOINT}"
    headers = {"apiToken": token, "Accept": "*/*"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: [!] Plugin config fetch failed: {e}")
        sys.exit(1)

# ====== Filter open ATR tickets with ID included ======
def get_open_ticket_data(tickets):
    data = {}
    for t in tickets:
        core = t.get("coreData", {})
        number = core.get("number")
        state = core.get("state", "").strip()
        last_update = core.get("lastUpdateDate")
        #ticket_id = t.get("id") or core.get("id")
        ticket_id = t.get("id") or t.get("coreData", {}).get("id")
        if number and state.lower() not in [s.lower() for s in EXCLUDE_STATES] and last_update and ticket_id:
            data[number] = {"state": state, "lastUpdateDate": last_update, "id": ticket_id}
    return data



# ====== Update tickets using Elasticsearch ======

def mark_orphans_in_elasticsearch(orphans):
    es = ATRElastic()

    for ticket_number, data in orphans.items():
        #ticket_id = data["id"]

        # Build search query
        query_body = get_query_body(ticket_number)
        try:
            search_result = es.client.search(index="*", body=query_body)
            hits = search_result.get("hits", {}).get("hits", [])

            if not hits:
                print(f"Error - ticket_not_found: {ticket_number}")
                continue

            source = hits[0]["_source"]
            doc_id = hits[0]["_id"]

            # Get ticket type
            ticket_type = source.get("fields", {}).get("atr_coredata_type", {}).get("value", "").lower()
            if not ticket_type:
                ticket_type = source.get("allFields", {}).get("type", "").lower()

            if not ticket_type:
                print(f"Error - ticket_type_unknown: {ticket_number}")
                continue

            index = get_index(ticket_type)

            # Set current UTC timestamp (ms)
            #current_timestamp = int(datetime.utcnow().timestamp() * 1000)

            # Conversion of 'Unix Epoch Milliseconds' format
            current_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

            update_body = {
                "doc": {
                    "fields": {
                        "atr_coredata_state": {
                            "value": "Orphan"
                        }
                    },
                    "updateDate": current_timestamp
                }
            }

            es.client.update(index=index, id=doc_id, body=update_body)
            print(f"ticket_marked_orphan: {ticket_number}")

        except Exception as e:
            print(f"Error - ticket_update_error: {ticket_number} - {e}")



# ====== Utility to parse datetime ======
def try_parse_date(date_str):
    formats = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

# ====== List ATR tickets by type with pagination ======
def list_tickets_by_type(token, base_url, ticket_type):
    all_tickets = []
    page = 0
    while True:
        url = (
            f"{base_url}/atr-gateway/ticket-management/api/v1/tickets"
            f"?ticketType={ticket_type}&sortDirection=DESC&page={page}&perPage={PER_PAGE}"
            f"&preset=default&isFuzzy=true&isScoreRequired=false"
        )
        headers = {"apiToken": token, "Accept": "*/*"}
        response = requests.get(url, headers=headers)

        # Break exits the loop  that’s iterating over pages, so it stops fetching further pages for that API call.
        if not response.ok:
            break
        page_data = response.json()

        #print(f"[DEBUG] Page {page} returned {len(page_data)} tickets.")  # Check tickets per_page

        if not isinstance(page_data, list):
            break
        all_tickets.extend(page_data)
        page += 1
    return all_tickets

# ======  Fetch Epic Keys (for Service Requests) ======
def fetch_epic_keys(jira_url, epic_jql, username, password):
    full_epic_jql = f'"issuetype"="Epic" AND ({epic_jql})'
    encoded_jql = quote(full_epic_jql)
    epic_url = f"{jira_url}/rest/api/2/search?jql={encoded_jql}&maxResults=500&startAt=0&fields=key"
    readable_url = unquote(epic_url)
    print(f"epic_url= {epic_url}")
    print(f"epic_readable_url = {readable_url}")
    try:
        response = requests.get(
            epic_url,
            auth=HTTPBasicAuth(username, password),
            headers={"Accept": "application/json"}
        )
        response.raise_for_status()
        epic_keys = [issue["key"] for issue in response.json().get("issues", [])]
        print(f"epic_keys= {json.dumps(epic_keys)}")
        return epic_keys
    except requests.exceptions.RequestException as e:
        print(f"[!] Epic API call failed: {e}")
        #return []
        return None  # <--- None means API failure


# ====== Fetch JIRA Ticket Data ======
def fetch_jira_data(jira_url, jql, username, password, fields, label):
    url = f"{jira_url}/rest/api/2/search?jql={quote(jql)}&maxResults=500&startAt=0&fields={fields}"
    readable_url = unquote(url)
    print(f"{label}_url= {url}")
    print(f"{label}_readable_url = {readable_url}")
    try:
        response = requests.get(url, auth=HTTPBasicAuth(username, password), headers={"Accept": "application/json"})
        response.raise_for_status()
        issues = response.json().get("issues", [])
        return {
            issue["key"]: {
                "state": issue.get("fields", {}).get("status", {}).get("name", "")
            }
            for issue in issues
            if issue.get("fields", {}).get("status", {}).get("name", "").lower() not in [s.lower() for s in EXCLUDE_STATES]
        }
    except requests.exceptions.RequestException as e:
        print(f"[!] JIRA API call failed: {e}")
        #return {}
        return None
    


def fetch_jira_data_for_epic_batches(
    jira_url, base_jql_parts, epic_keys, username, password, fields, label, batch_size=MAX_EPIC_BATCH_SIZE
):
    """
    Batches the epic_keys list (range) and reuses fetch_jira_data()
    per batch, merging results. Returns None if any batch fails.
    """
    merged = {}
    batch_no = 0

    for start in range(0, len(epic_keys), batch_size):
        batch_no += 1
        batch = epic_keys[start:start + batch_size]

        quoted_keys = ",".join(f'"{k}"' for k in batch)
        epic_filter = f'"Epic Link" IN ({quoted_keys})'
        jql = " AND ".join([p for p in (base_jql_parts + [epic_filter]) if p])

        data = fetch_jira_data(
            jira_url, jql, username, password, fields, f"{label}_EPICBATCH{batch_no}"
        )
        if data is None:
            print(f"ERROR: {label} batch {batch_no} failed; aborting SR fetch.")
            return None

        merged.update(data)

    print(f"{label}_epic_batches_total={batch_no}")
    return merged




# ====== MAIN ======
if __name__ == "__main__":

    overall_status = "success"

    if len(sys.argv) < 3:
        print("Usage: script.py <JIRA_USERNAME> <JIRA_PASSWORD>. ERROR: Missing input parameters.")
        sys.exit(1)

    jira_username = sys.argv[1]
    jira_password = sys.argv[2]

    if not jira_username.strip() or not jira_password.strip():
        print("ERROR: [!] Jira username or password is empty. Aborting.")
        sys.exit(1)

    token, base_url = get_token_and_baseurl()

    # ====== Validate ATR token and base URL ======
    if not token or not base_url:
        print("ERROR: [!] Failed to retrieve ATR token or base URL. Aborting.")
        sys.exit(1)

    plugin_config = retrieve_plugin_configuration(token, base_url)
    fields = plugin_config.get("fields", [])

    def get_field_value(field_id):
        return next((f.get("value") for f in fields if f.get("id") == field_id), None)

    jira_homeurl            = get_field_value("JIRA_HOMEURL")
    srequest_fields         = get_field_value("JIRA_FIELDS_SREQUEST")
    bug_fields              = get_field_value("JIRA_FIELDS_BUG")
    srequest_issuetype      = get_field_value("JIRA_SREQUEST_ISSUE_TYPES")
    bug_projects            = get_field_value("JIRA_PROJECTS_BUG")
    epic_jql                = get_field_value("JIRA_SREQUEST_EPIC")
    use_epic_switch         = get_field_value("JIRA_SREQUEST_SWITCH") == "true"  # Convert use_epic_switch to boolean
    #use_epic_switch         = str(get_field_value("JIRA_SREQUEST_SWITCH")).strip().lower() == "true"
    bug_filters             = get_field_value("JIRA_FILTERS_BUG")
    srequest_filters        = get_field_value("JIRA_FILTERS_SREQUEST")
    jql_filters_by_type     = get_field_value("JIRA_JQL_FILTERS")
    sreq_project_itypes     = get_field_value("JIRA_SREQ_PROJECT_ITYPES")

    # Validate plugin config fields

    
    try:
        bug_filter_jql = json.loads(bug_filters).get("jql") if bug_filters else None
    except json.JSONDecodeError:
        bug_filter_jql = None



    try:
        if srequest_filters:
            try:
            # Try parsing as JSON to get the "jql" key
                srequest_filter_jql = json.loads(srequest_filters).get("jql")
            except json.JSONDecodeError:
            # Fallback to raw string
                srequest_filter_jql = srequest_filters.strip()
        else:
            srequest_filter_jql = None
    except Exception:
        srequest_filter_jql = None



    try:
        jql_filter_map = json.loads(jql_filters_by_type) if jql_filters_by_type else {}
    except json.JSONDecodeError:
        jql_filter_map = {}

    try:
        sreq_project_itype_map = json.loads(sreq_project_itypes) if sreq_project_itypes else {}
    except json.JSONDecodeError:
        sreq_project_itype_map = {}

    if not jira_homeurl:
        print("ERROR: [!] Missing JIRA_HOMEURL in plugin config. Aborting.")
        sys.exit(1)

    if not srequest_fields or not srequest_issuetype:
        print("[!] Missing Service Request config fields. Skipping SR fetch.")
    if not bug_fields or not bug_projects:
        print("[!] Missing Bug config fields. Skipping Bug fetch.")


    # --- EPIC handling ---
    skip_sr_due_to_epic_failure = False

    if use_epic_switch and not epic_jql:
        print("[!] Epic JQL (JIRA_SREQUEST_EPIC) is missing but switch is enabled. Skipping Epic fetch.")
        epic_keys = []


    elif use_epic_switch and epic_jql:
        epic_keys = fetch_epic_keys(jira_homeurl, epic_jql, jira_username, jira_password)
        if epic_keys is None:  # API failed
            print("ERROR: Failed to retieve Epic Keys, Skipping Orphan Identification for Service Request")
            overall_status = "Failure"
            skip_sr_due_to_epic_failure = True
            epic_keys = []
        elif len(epic_keys) == 0:
            print("ERROR: Empty Epic Keys retrieved, Skipping Orphan Identification for Service Request")
            overall_status = "Failure"
            skip_sr_due_to_epic_failure = True
            epic_keys = []
    else:
        epic_keys = []



    types = []
    if srequest_fields and srequest_issuetype and not skip_sr_due_to_epic_failure:
        types.append(("JIRA_SERVICE_REQUEST", "SREQUEST", srequest_fields, srequest_issuetype))
    if bug_fields and bug_projects:
        types.append(("JIRA_BUG", "BUG", bug_fields, "Bug"))

    for ticket_type, short_type, field_str, issue_type in types:
        print(f"Fetching tickets for type: {short_type}")
        atr_tickets = list_tickets_by_type(token, base_url, ticket_type)
        atr_open_data = get_open_ticket_data(atr_tickets)

        print(f"{short_type}_open_ticket_count={len(atr_open_data)}")
        print(f"{short_type}_open_ticket_data=")
        print(json.dumps(atr_open_data, indent=2))

        dates = [try_parse_date(v["lastUpdateDate"]) for v in atr_open_data.values() if try_parse_date(v["lastUpdateDate"])]
        if not dates:
            print(f"[INFO] No open ATR tickets for {short_type}. Skipping Orphan Identification for {short_type}")
            continue    # skip to next ticket type and Skips the rest of the code for this ticket type.

        oldest = min(dates).strftime("%Y-%m-%d %H:%M")
        print(f"{short_type}_oldest_lastUpdateDate={oldest}")

        

        if ticket_type == "JIRA_SERVICE_REQUEST":
            # ----- NEW PROJECT FILTER BASED ON ISSUE TYPE -----
            project_list_str = sreq_project_itype_map.get(issue_type.lower())
            if project_list_str:
                project_list = ",".join([f'"{proj.strip()}"' for proj in project_list_str.split(",")])
                project_filter = f'project IN ({project_list})'
            else:
                project_filter = ''

            #  # Build Epic Filter safely
            # if use_epic_switch and epic_keys:
            #     quoted_keys = ','.join(f'"{k}"' for k in epic_keys)
            #     epic_filter = f'"Epic Link" IN ({quoted_keys})'
            # else:
            #     epic_filter = ''


            custom_type_filter = jql_filter_map.get(issue_type.lower()) # Only for 'SRequest'

            #  Build dynamic JQL for SRequest
            jql_parts = [
                project_filter,
                f'issuetype="{issue_type}"',
                #epic_filter,
                f'updated > "{oldest}"',
                srequest_filter_jql,
                custom_type_filter
            ]


            if use_epic_switch and epic_keys:
                print("Length Epic Keys: ", len(epic_keys))
                # If the epic list is large, do multiple GETs and merge
                if len(epic_keys) > MAX_EPIC_BATCH_SIZE:
                    print("Length Epic Keys greater than MAX_EPIC_BATCH_SIZE")
                    jira_data = fetch_jira_data_for_epic_batches(
                        jira_homeurl, jql_parts, epic_keys,
                        jira_username, jira_password, field_str, short_type, MAX_EPIC_BATCH_SIZE
                    )
                else:
                # Small list — single GET like before
                    quoted_keys = ",".join(f'"{k}"' for k in epic_keys)
                    epic_filter = f'"Epic Link" IN ({quoted_keys})'
                    jql = " AND ".join([p for p in (jql_parts + [epic_filter]) if p])
                    jira_data = fetch_jira_data(
                        jira_homeurl, jql, jira_username, jira_password, field_str, short_type
                    )
            else:
                # No Epic keys / Epic switch OFF → skip SRequest fetch
                print("[INFO] No Epic keys or Epic switch is OFF; Skipping Orphan Identification for SREQUEST")
                continue  # move to next ticket type
                


        else:
            project_list = ",".join([f'"{proj.strip()}"' for proj in bug_projects.split(",")])   # Build dynamic JQL for 'Bug'
            jql_parts = [
                f'("project" IN ({project_list}))',
                f'"issuetype"="{issue_type}"',
                f'"updated">"{oldest}"',
                bug_filter_jql            
            ]

            jql = " AND ".join([p for p in jql_parts if p])
            jira_data = fetch_jira_data(jira_homeurl, jql, jira_username, jira_password, field_str, short_type)

        # If Jira API/URL failed, do NOT orphan everything—skip this type safely.
        if jira_data is None:
            print(f"ERROR: JIRA API call for {short_type} Failed, Skipping Orphan Identification for {short_type}")
            overall_status = "Failure"
            continue  # move on to the next ticket type

        print(f"{short_type}_JIRA_open_ticket_count={len(jira_data)}")
        print(f"{short_type}_JIRA_open_ticket_data=")
        print(json.dumps(jira_data, indent=2))

        orphans = {k: v for k, v in atr_open_data.items() if k not in jira_data}
        print(f"{short_type}_orphan_count={len(orphans)}")
        print(f"{short_type}_orphan_data=")
        print(json.dumps(orphans, indent=2))


        # ========== USE ELASTIC DELETION HERE ==========

        mark_orphans_in_elasticsearch(orphans)

    if overall_status=="Failure":
        sys.exit(1)


