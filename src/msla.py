from __future__ import annotations

import os
import requests
import base64
import msal
import jwt
import json
import datetime as dt
import polars as pl

from typing import Dict, List, Optional, Any, Iterable, Union

from src.config import (
    APPLICATION_ID, SECRET_VALUE_ID, AUTHORITY, SCOPES, GRAPH_BASE,
    SHARED_MAILS, EMAIL_COLUMNS, SHARED_MAIL_1,
    #MS, GS, SAXO, EDB, UBS,
    COUNTERPARTIES
)

def date_to_str (date : Optional[str | dt.datetime | dt.date] = None, format : str = "%Y-%m-%d") :
    
    if date is None :
        date = dt.datetime.now()

    if isinstance(date, str) :
        return str(date)

    return date.strftime(format)


def get_token (
        
        scopes : Optional[List] = None,
        app_id : Optional[str] =  None,
        authority : Optional[str] = None,
        secret :  Optional[str] = None
    
    ) -> Optional[str] :
    """
    Function get token from the applcation 
    """
    scopes = SCOPES if scopes is None else scopes

    app_id = APPLICATION_ID if app_id is None else app_id
    authority = AUTHORITY if authority is None else authority
    secret = SECRET_VALUE_ID if secret is None else secret
    
    app = msal.ConfidentialClientApplication(

        client_id=app_id,
        authority=authority,
        client_credential=secret

    )

    result = app.acquire_token_for_client(

        scopes=scopes

    )

        
    if "access_token" in result :

        print("\n[+] Token acquired successfully")
        print(result["access_token"][:30] + "...")  # Print just token first 30 letters
    
    else :

        print("\n[-] Failed to acquire token\n")
        print(result.get("error_description"))

    return result.get("access_token", None)


def decode_token (token : str) -> List[Dict[str, Any]] :

    if token is None :
        return None
    
    decoded = jwt.decode(token, options={"verify_signature": False})
    
    print("\n[*] Token claims :")
    print("\t[*] roles: ", decoded.get("roles"))
    print("\t[*] App Id:", decoded.get("appid"))
    
    return decoded


def get_inbox_messages_between (

        start_date : Optional[str | dt.datetime | dt.date] = None,
        end_date : Optional[str | dt.datetime | dt.date] = None,
        token : Optional[str] = None,
        email : Optional[str] = None,
        graph_base : Optional[str] = None,
        with_attach : bool = False

    ) :
    """
    
    """
    token = get_token() if token is None else token
    graph_base = GRAPH_BASE if graph_base is None else graph_base
    email = SHARED_MAILS[0] if email is None else email

    start_date = date_to_str(start_date)
    end_date = date_to_str(end_date)

    filter_str = f"receivedDateTime ge {start_date}"

    parameters = {
        
        "$orderby": "receivedDateTime ASC",
        "$select": "id,subject,from,receivedDateTime,hasAttachments",
        "$filter": filter_str,
        "$top": "100"

    }

    if with_attach is True :
        
        # Only metadata (id, name, contentType, size, isInline)
        parameters["$expand"] = "attachments($select=id,name,contentType,size,isInline)"


    headers = {
    
        "Authorization": f"Bearer {token}"
        
    }

    url = f"{graph_base}/users/{email}/mailFolders/Inbox/messages"

    rows: List[dict] = []
    while True :

        response = requests.get(
            
            url=url,
            headers=headers,
            params=parameters

        )

        if response.status_code != 200 :
            raise Exception(f"Graph API error {response.status_code}: {response.text}")
        
        data = response.json()

        for m in data.get("value", []) :

            rows.append(
            
                {
                    "Id": m.get("id"),
                    "Subject": m.get("subject"),
                    "From": m.get("from", {}).get("emailAddress", {}).get("address"),
                    "Received DateTime": m.get("receivedDateTime"),
                    "Attachments": m.get("hasAttachments"),
                    "Shared Email" : str(email)
                }
            
            )

        next_link = data.get("@odata.nextLink")

        if not next_link :
            #print(f"Break here for {next_link}")
            break
        
        url = next_link
        params = None  # already encoded in nextLink

    df_email = pl.DataFrame(rows, schema_overrides=EMAIL_COLUMNS)

    return df_email


def download_attachments_for_message (

        message_id: str,
        token : Optional[str] = None,
        out_dir : Optional[str] = "attachments",
        user_upn: Optional[str] = None,
        attachment : Optional[str] = "/attachments"
    
    ):
    """
    message_id: the Graph message id (string)
    user_upn: optional, e.g. 'alice@example.com'; when provided use /users/{user_upn}/messages/{id}
              otherwise uses /me/messages/{id}
    """
    token = get_token() if token is None else token
    user_upn = SHARED_MAIL_1 if user_upn is None else user_upn

    os.makedirs(out_dir, exist_ok=True)

    headers = {"Authorization": f"Bearer {token}"}
    base = f"{GRAPH_BASE}/users/{user_upn}/messages/{message_id}"

    # List attachments
    list_url = base + attachment
    r = requests.get(
    
        list_url,
        headers=headers
    
    )

    r.raise_for_status()
    
    attachments = r.json().get("value", [])

    if not attachments :

        print("[-] No attachments found.")
        return []

    saved = []

    for att in attachments :

        att_id = att.get("id")
        att_name = att.get("name") or att.get("contentType") or f"attachment-{att_id}"
        odata_type = att.get("@odata.type", "")

        # fileAttachment: contains contentBytes (base64)
        if odata_type.lower().endswith("fileattachment") :

            content_b64 = att.get("contentBytes")
            
            if content_b64 :

                data = base64.b64decode(content_b64)
                path = os.path.join(out_dir, att_name)
                
                with open(path, "wb") as f :
                    f.write(data)

                saved.append(path)
                print("[*] Saved file Attachment at ", path)

            else :

                # fallback: fetch full attachment by id
                get_url = f"{list_url}/{att_id}"
                
                rr = requests.get(
                
                    get_url,
                    headers=headers
                
                )

                rr.raise_for_status()
                
                full = rr.json()
                cb = full.get("contentBytes")
                
                if cb :

                    path = os.path.join(out_dir, full.get("name", att_name))
                    
                    with open(path, "wb") as f :
                        f.write(base64.b64decode(cb))

                    saved.append(path)
                    print("[+] Saved (fetched) fileAttachment ->", path)
                
                else :
                    print("[*] Attachment missing contentBytes:", att_id)

        # itemAttachment: embedded message/event/contact (may contain an 'item' property)
        elif odata_type.lower().endswith("itemattachment") :

            # You can GET the attachment by id to inspect the embedded item
            get_url = f"{list_url}/{att_id}"
            
            rr = requests.get(
            
                get_url,
                headers=headers
            
            )

            rr.raise_for_status()
            item = rr.json().get("item")
            
            # Save the embedded item's subject/body as .eml or .json
            fname = att.get("name") or f"embedded-{att_id}.json"
            path = os.path.join(out_dir, fname)
            
            with open(path, "w", encoding="utf-8") as f :
                json.dump(item, f, ensure_ascii=False, indent=2)
            
            saved.append(path)
            print("Saved itemAttachment (JSON) ->", path)

        # referenceAttachment: link to content in cloud (OneDrive/SharePoint etc.)
        elif odata_type.lower().endswith("referenceattachment") :

            # referenceAttachment contains a 'sourceUrl' or other metadata
            src = att.get("sourceUrl") or att.get("contentLocation")
            
            info_path = os.path.join(out_dir, f"reference-{att_id}.txt")
            
            with open(info_path, "w", encoding="utf-8") as f :
                f.write(f"Reference attachment metadata:\n{att}\n\nSource URL: {src}\n")
            
            saved.append(info_path)
            print("Saved referenceAttachment metadata ->", info_path)

        else:
            # Unknown type: try fetching by id
            get_url = f"{list_url}/{att_id}"
            
            rr = requests.get(get_url, headers=headers)
            rr.raise_for_status()
            
            with open(os.path.join(out_dir, f"unknown-{att_id}.json"), "w", encoding="utf-8") as f:
                import json
                json.dump(rr.json(), f, ensure_ascii=False, indent=2)
            print("Saved unknown attachment metadata for inspection:", att_id)

    return saved
    
