import requests
import json
from .TermOutput import msg
from .Cache import cachedRequest

@cachedRequest("DOI")
def crossrefLookup(doi):
    url = "https://api.crossref.org/works/"+doi
    response=None
    decoded=None

    try:
        response = requests.get(url).text
        if response == 'Resource not found.':
            return None
        decoded = json.loads(response)
        if decoded['status'] != 'ok':
            raise RuntimeError("Crossref API call failed")
        return decoded['message']
    except:
        msg("doi=%s\n\nresponse=%s\n\ndecoded=%s\n\n",doi,response,decoded)
        raise
