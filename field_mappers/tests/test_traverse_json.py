from field_mappers.base import get_value_at_json_path  # Make sure to import get_value_at_json_path from its module

def test_nested_dict():
    json_input = {"person": {"name": "John", "age": 30}}
    assert get_value_at_json_path(json_input, "person.name") == "John"

def test_nested_structure():
    json_input = {
        "users": [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25}
        ]
    }
    assert get_value_at_json_path(json_input, "users[0].age") == 30
