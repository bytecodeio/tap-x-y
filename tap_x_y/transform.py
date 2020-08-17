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


def denest(this_json):
    new_json = {}
    index = 0
    if isinstance(this_json, dict):
        for key, value in this_json.items():
            if isinstance(this_json[key], dict):
                for child_key, child_value in this_json[key].items():
                    if child_key == '$uri':
                        new_key = key
                    else:
                        new_key = key + '_' + child_key
                    new_json[new_key]= child_value
            else:
                new_json[key] = value
    elif isinstance(this_json, list):
        for item in this_json:
            denest(item)
        
    return new_json

def transform(this_json):
    snake = convert_json(this_json)
    denested = [denest(nested) for nested in snake]
    # denested = denest(snake)
    return convert_json(denested)