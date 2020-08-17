import humps
import re
import singer


LOGGER = singer.get_logger()


# Convert camelCase to snake_case and remove forward slashes
def convert(name):
    regsub = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    unslash = re.sub('/', '_', regsub)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', unslash).lower()


# Convert keys in json array
def convert_array(arr):
    new_arr = []
    for i in arr:
        if isinstance(i, list):
            new_arr.append(convert_array(i))
        elif isinstance(i, dict):
            new_arr.append(convert_json(i))
        else:
            new_arr.append(i)
    return new_arr


# Convert keys in json
def convert_json(this_json):
    out = {}
    if isinstance(this_json, dict):
        for key in this_json:
            if key == 'items':
                new_key = 'list_items'
            else:
                new_key = convert(key)
            if isinstance(this_json[key], dict):
                out[new_key] = convert_json(this_json[key])
            elif isinstance(this_json[key], list):
                out[new_key] = convert_array(this_json[key])
            else:
                out[new_key] = this_json[key]
    else:
        return convert_array(this_json)
    return out


def denest(this_json, data_key, denest_keys):
    """Denest by path and key. Moves all elements at key to parent level if
    no target provided. Target provided by dot syntax, e.g. key.target.
    Arguments:
        this_json {[type]} -- [description]
        data_key {[type]} -- [description]
        denest_keys {[type]} -- [description]
    Raises:
        AssertionException: If denested key exists in parent.
    Returns:
        json -- Transformed json with denested keys.
    """
    new_json = this_json
    index = 0
    for record in list(this_json.get(data_key, [])):
        for denest_key in denest_keys.split(","):
            if "." in denest_key:
                denest_targeted_nodes(
                    index, data_key, record, new_json, denest_key)
            else:
                denest_node_all_elements(
                    index, record, denest_key, data_key, new_json)
        index = index + 1
    return new_json

def transform(this_json):
    return convert_json(this_json)