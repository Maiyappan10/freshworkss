import os
import json
import time
import lockfile

class DataStore:

    file_path = None
    
    # Create file in specified file path
    def __init__(self, f=0):

        # Reliable default path
        if f==0:
            directory = "DataStore"
            file_path = os.path.join('C:/', directory)
            if os.path.isfile(file_path+'/data.json'):
                self.file_path = file_path+'/data.json'
                print("Success : File exists. File path is ", file_path+'/data.json')

            else:                                            
                os.mkdir(file_path)
                with lockfile.LockFile(file_path+'/data.json'):
                    with open(file_path+'/data.json', "w") as outfile: 
                        json.dump({}, outfile)

                self.file_path = file_path+'/data.json'
                print('Success : File path is ', file_path+'/data.json')                

        # Custom path
        else:            
            if os.path.isfile(f+'/data.json'):
                self.file_path = f+'/data.json'
                print("Success : File exists. File path is ", f+'/data.json')                

            else:
                with lockfile.LockFile(f+'/data.json'):
                    with open(f+'/data.json', "w") as outfile: 
                        json.dump({}, outfile)

                    self.file_path = f+'/data.json'
                    print('Success : File path is ', f+'/data.json')                                                    

    # Read content from json file                    
    def read_data_file(self):

        # File locked
        with lockfile.LockFile(self.file_path):
            with open(self.file_path, "r") as json_data:
                return json.load(json_data, strict=False)

    # Write content in json file
    def write_data_file(self, content):

        # File locked
        with lockfile.LockFile(self.file_path):
            with open(self.file_path, "w") as outfile: 
                json.dump(content, outfile)
                return True
        
    # Create key and store in datastore file
    def create(self, key, value, timeout=0):

                # Read content from datastore file                                    
                d = self.read_data_file()
                                           
                if key in d:
                    print("Error: "+key+" key already exists")
                    
                else:                    

                    # Check key is alphabets or not
                    if (type(key) == str):

                        # Check length of datastore file (MAX_LIMIT = 1GB) and object value (MAX_LIMIT = 16KB)
                        if len(d) < (1024 * 1020 * 1024) and len(value) <= (16 * 1024 * 1024):                            

                            # Set Time-To-Live property
                            if timeout == 0:
                                l = [value, timeout]                                
                            else:                                
                                l = [value, time.time() + timeout]
                            
                            if len(key) <= 32:
                                                                
                                # Read content from datastore file                                    
                                content = self.read_data_file()                                    
                                content[key] = l

                                # Write content from datastore file                                    
                                #content = write_data_file(file_path, content)

                                if self.write_data_file(content):
                                    print("Success: Key created successfully")
                                else:
                                    print("Error: Key can't create") 
                                    
                        else:
                            print("Error: Memory limit exceeded!! ")
                    else:
                        print("Error: Invalind key_name!! key_name must contain only alphabets and no special characters or numbers")
            
                        
        
    def read(self, key):

        # Read content from datastore file                                    
        d = self.read_data_file()
        
        if key not in d:
            return "Error: given key does not exist in database. Please enter a valid key"

        else:
            b = d[key]            
            if b[1] != 0:

                # comparing the current time with expiry time                
                if time.time() < b[1]:
                    temp_dict = dict()
                    temp_dict[key] = str(b[0])
                    return temp_dict
                else:
                    return "Error: time-to-live of" + key+ " has expired"  

            else:
                temp_dict = dict()
                temp_dict[key] = str(b[0])            
                return temp_dict
            
    def delete(self, key):

        # Read content from datastore file                                    
        d = self.read_data_file()
        
        if key not in d:
            print("Error: Given key does not exist in database. Please enter a valid key") 
        else:
            b = d[key]
            if b[1] != 0:

                # comparing the current time with expiry time
                if time.time() < b[1]:  
                    del d[key]
                    if self.write_data_file(d):
                        print("Success:"+key+" Key deleted successfully")
                    else:
                        print("Error: Key can't delete")                                                        

                else:
                    print("error: time-to-live of", key, "has expired") 

            else:
                del d[key]
                if self.write_data_file(d):
                    print("Success: "+key+" Key deleted successfully")
                else:
                    print("Error: Key can't delete")                                                        
            
