from importlib import import_module


def import_from_module(name, package=None):
    parts = name.rsplit(".", 1)
    module_name = parts[0]
    var_name = None
    if len(parts) > 1:
        var_name = parts[1]
    mod = import_module(module_name, package)
    if var_name is None:
        return mod
    else:
        return mod.get(var_name)
