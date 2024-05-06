import importlib.util
import sys
import os
from pathlib import Path
import re


curfile = Path(os.path.abspath(__file__))
pkg_dir = curfile.parent.parent
pkg_setup_file = os.path.join(pkg_dir, 'setup.py')
src_dir = os.path.join(pkg_dir, 'src')
pkg_dir = [d for d in os.listdir(src_dir) if '.' not in d][0]
pkg_init_file = os.path.join(src_dir, pkg_dir, '__init__.py')


def get_version() -> str:
    spec = importlib.util.spec_from_file_location("pkg_setup", pkg_setup_file)
    pkg_setup = importlib.util.module_from_spec(spec)
    sys.modules["pkg_setup"] = pkg_setup
    spec.loader.exec_module(pkg_setup)
    return pkg_setup.VERSION


def inc_version(ver: str) -> str:
    last_digit = re.findall(r'\d+', ver)[-1]
    new_version = ver[:-(len(last_digit))] + str(int(last_digit) + 1)
    return new_version


def update_version():
    old_version = get_version()
    new_version = inc_version(old_version)

    with open(pkg_setup_file, 'r') as pkg:
        pkg_text: str = pkg.read()
        pkg_text = pkg_text.replace(old_version, new_version)
        pkg.close()
    
    with open(pkg_setup_file, 'w') as pkg:
        pkg.write(pkg_text)
        pkg.close()

    with open(pkg_init_file, 'r') as init:
        init_text: str = init.read()
        if len(re.findall(r'__version__(\s*)=(\s*)(\'|").*(\'|")', init_text)):
            init_text = re.sub(r'__version__(\s*)=(\s*)(\'|").*(\'|")', f'__version__ = "{new_version}"', init_text)
        else:
            init_text += f'\n__version__ = "{new_version}"\n'
    
    with open(pkg_init_file, 'w') as init:
        init.write(init_text)
        init.close()


if __name__ == '__main__':
    update_version()


