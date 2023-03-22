import re

def normalize_name(name):
    """Remove all special characters and use abbreviations"""
    not_german_letters = "[^A-Za-zÄäÖöÜüß]+"
    replace_dict = {
        "Kr ": "",
        "Hauptbahnhof": "Hbf",
        "Bahnhof": "bf",
        "bahnhof": "bf",
        ' bei ':"b",
        "(bei": "b"}
    for search, repl in replace_dict.items():
        name= name.replace(search, repl)
    
    # Remove all special characters
    name = re.sub(not_german_letters, '', name)
    return name