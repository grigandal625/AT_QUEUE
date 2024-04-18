import inspect
from typing import Union, Callable, _TypedDictMeta
from collections import OrderedDict

# Mapping from Python type annotations to JSON schema types
python_to_json_schema_types = {
    int: "integer",
    float: "number",
    str: "string",
    bool: "boolean",
    list: "array",
    dict: "object",
    type(None): "null"
    # Add more mappings as needed
}

def get_json_schema_type(annotation):
    if annotation is None:
        return {"type": "null"}
    if hasattr(annotation, "__origin__") and annotation.__origin__ is not None:
        origin = annotation.__origin__
        if origin == Union:
            return {
                "anyOf": [get_json_schema_type(t) for t in annotation.__args__]
            }
        elif origin == list:
            item_type = get_json_schema_type(annotation.__args__[0])
            return {"type": "array", "items": item_type}
        # Add support for more complex types as needed
    elif isinstance(annotation, _TypedDictMeta):
        properties = {}
        for key, value in annotation.__annotations__.items():
            properties[key] = get_json_schema_type(value)
        return {"type": "object", "properties": properties}
    return {'type': python_to_json_schema_types.get(annotation, "string")} 


def function_to_json_schema(func: Callable, as_method=False):
    signature = inspect.signature(func)
    schema = {
        "type": "object",
        "properties": OrderedDict(),
        "required": []
    }
    
    for param_name, param in (list(signature.parameters.items())[1:] if as_method else signature.parameters.items()):
        param_schema = {"type": "string"}  # Default type is string
        if param.annotation != inspect.Parameter.empty:
            param_type = get_json_schema_type(param.annotation)
            param_schema = param_type
        if param.default == inspect.Parameter.empty:
            schema["required"].append(param_name)
        else:
            param_schema["default"] = param.default
        schema["properties"][param_name] = param_schema
    
    return schema


def return_type_to_json_schema(func: Callable):
    signature = inspect.signature(func)
    return_annotation = signature.return_annotation

    if return_annotation == inspect.Signature.empty:
        return None

    return get_json_schema_type(return_annotation)


def method_to_json_schema(method: Callable):
    return function_to_json_schema(method, as_method=True)