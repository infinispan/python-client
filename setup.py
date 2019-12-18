import os
import pathlib
import shutil

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as build_ext_orig
from setuptools.command.build_py import build_py as build_py_orig
from setuptools.command.egg_info import egg_info as egg_info_orig
from pprint import pprint
from sys import platform

class CMakeExtension(Extension):

    def __init__(self, name):
        # don't invoke the original build_ext for this special extension
        super().__init__(name, sources=[])

class build_py(build_py_orig):
    def run(self):
        self.run_command("build_ext")
        return super().run()

class egg_info(egg_info_orig):
    def run(self):
        cwd = pathlib.Path().absolute()
        package_dir = pathlib.Path('Infinispan')
        package_dir.mkdir(parents=True, exist_ok=True)
        super().run()

class build_ext(build_ext_orig):

    def run(self):
        for ext in self.extensions:
            self.build_cmake(ext)
        super().run()

    def build_cmake(self, ext):
        cwd = pathlib.Path().absolute()

        # these dirs will be created in build_py, so if you don't have
        # any python sources to bundle, the dirs will be missing
        package_dir = pathlib.Path('Infinispan')
        package_dir.mkdir(parents=True, exist_ok=True)
        build_temp = pathlib.Path(self.build_temp)
        build_temp.mkdir(parents=True, exist_ok=True)
        extdir = pathlib.Path(self.get_ext_fullpath(ext.name))
        extdir.mkdir(parents=True, exist_ok=True)

        # example of cmake args
        cmake_args = [
            '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + str(extdir.parent.absolute()),
        ]

        os.chdir(str(build_temp))
        self.spawn(['cmake', str(cwd)] + cmake_args)
        if not self.dry_run:
            self.spawn(['cmake', '--build', '.'] )
            print(os.getcwd())
            dirlist=os.listdir('..')
            pprint(dirlist)
        os.chdir(str(cwd))


if platform == "linux" or platform == "linux2":
    lib_prefix="lib"
    lib_suffix=".so"
    lib_path = 'build/cpp-client/src/cppclient-build/'+lib_prefix+'hotrod'+lib_suffix+".1.0"
elif platform == "darwin":
    lib_prefix="lib"
    lib_suffix=".dylib"
    lib_path = 'build/cpp-client/src/cppclient-build/'+lib_prefix+'hotrod'+".1.0"+lib_suffix
elif platform == "win32":
    print("Windows is not yet supported")
    quit()
setup(
    name='infinispan',
    version='0.1',
    packages=['Infinispan'],
    data_files=[('lib', [lib_path])],
    ext_modules=[CMakeExtension('Infinispan/Infinispan')],
    cmdclass={
        'build_ext': build_ext,
        'build_py' : build_py, 'egg_info': egg_info},
)
