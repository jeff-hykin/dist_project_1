import sys
import os
from os.path import isabs, isfile, isdir, join, dirname, basename, exists, splitext, relpath
from os import remove, getcwd, makedirs, listdir, rename, rmdir, system
from shutil import move
import glob
import time
import shutil


# 
# create a class for generate filesystemtem management
# 
class FileSystem():
    @classmethod
    def size_of(self, file_path):
        import os
        if self.is_dir(file_path):
            raise Exception('Sorry FS.size_of() currently does not work on folders')
            
        file = open(file_path)
        file.seek(0, os.SEEK_END)
        number_of_bytes = file.tell()
        file.close()
        return number_of_bytes
    
    @classmethod
    def write(self, data, to=None):
        # make sure the path exists
        self.makedirs(os.path.dirname(to))
        with open(to, 'w') as the_file:
            the_file.write(str(data))
    
    @classmethod
    def read(self, file_path, into="string"):
        try:
            if into == "string":
                with open(file_path,'r') as f:
                    output = f.read()
            elif into == "binary_array":
                size = self.size_of(file_path)
                from array import array
                output = array('B')
                with open(file_path, 'rb') as f:
                    output.fromfile(f, size)
        except:
            output = None
        return output    
        
    @classmethod
    def delete(self, file_path):
        if isdir(file_path):
            shutil.rmtree(file_path)
        else:
            try:
                os.remove(file_path)
            except:
                pass
    
    @classmethod
    def makedirs(self, path):
        try:
            os.makedirs(path)
        except:
            pass
        
    @classmethod
    def copy(self, from_=None, to=None, new_name="", force= True):
        if new_name == "":
            raise Exception('self.copy() needs a new_name= argument:\n    self.copy(from_="location", to="directory", new_name="")\nif you want the name to be the same as before do new_name=None')
        elif new_name is None:
            new_name = os.path.basename(from_)
        
        # get the full path
        to = os.path.join(to, new_name)
        # if theres a file in the target, delete it
        if force and self.exists(to):
            self.delete(to)
        # make sure the containing folder exists
        self.makedirs(os.path.dirname(to))
        if os.path.isdir(from_):
            shutil.copytree(from_, to)
        else:
            return shutil.copy(from_, to)
    
    @classmethod
    def move(self, from_=None, to=None, new_name="", force= True):
        if new_name == "":
            raise Exception('self.move() needs a new_name= argument:\n    self.move(from_="location", to="directory", new_name="")\nif you want the name to be the same as before do new_name=None')
        elif new_name is None:
            new_name = os.path.basename(from_)
        
        # get the full path
        to = os.path.join(to, new_name)
        # make sure the containing folder exists
        self.makedirs(os.path.dirname(to))
        shutil.move(from_, to)
    
    @classmethod
    def exists(self, *args):
        return self.does_exist(*args)
    
    @classmethod
    def does_exist(self, path):
        return os.path.exists(path)
    
    @classmethod
    def is_folder(self, *args):
        return self.is_directory(*args)
        
    @classmethod
    def is_dir(self, *args):
        return self.is_directory(*args)
        
    @classmethod
    def is_directory(self, path):
        return os.path.isdir(path)
    
    @classmethod
    def is_file(self, path):
        return os.path.isfile(path)

    @classmethod
    def list_files(self, path="."):
        return [ self.basename(x) for x in self.ls(path) if self.is_file(x) ]
    
    @classmethod
    def list_folders(self, path="."):
        return [ self.basename(x) for x in self.ls(path) if self.is_folder(x) ]
    
    @classmethod
    def ls(self, file_path="."):
        glob_val = file_path
        if os.path.isdir(file_path):
            glob_val = os.path.join(file_path, "*")
        return glob.glob(glob_val)

    @classmethod
    def glob(self, file_path):
        return glob.glob(file_path)

    @classmethod
    def touch(self, path):
        self.makedirs(self.dirname(path))
        if not self.exists(path):
            self.write("", to=path)
    
    @classmethod
    def touch_dir(self, path):
        self.makedirs(path)

    @classmethod
    def create_folder(self, path):
        self.makedirs(path)

    @classmethod
    def create_file(self, path):
        self.touch(path)
    
    @classmethod
    def dirname(self, path):
        return os.path.dirname(path)
    
    @classmethod
    def basename(self, path):
        return os.path.basename(path)
    
    @classmethod
    def extname(self, path):
        filename, file_extension = os.path.splitext(path)
        return file_extension
    
    @classmethod
    def path_pieces(self, path):
        """
        example:
            *folders, file_name, file_extension = self.path_pieces("/this/is/a/file_path.txt")
        """
        folders = []
        while 1:
            path, folder = os.path.split(path)

            if folder != "":
                folders.append(folder)
            else:
                if path != "":
                    folders.append(path)

                break
        folders.reverse()
        last = folders.pop()
        filename, file_extension = os.path.splitext(file)
        return folders + [ filename, file_extension ]
    
    @classmethod
    def join(self, *paths):
        return os.path.join(*paths)
    
    @classmethod
    def absolute_path(self, path):
        return os.path.abspath(path)

    @classmethod
    def pwd(self):
        return os.getcwd()
    
    @classmethod
    def json_read(self, file_path):
        import json
        with open(file_path,) as f:
            data = json.load(f) 
        return data
            
    @classmethod
    def json_write(self, data, to):
        import json
        with open(to, "w") as outfile:
            json.dump(data, outfile)

FS = FileSystem