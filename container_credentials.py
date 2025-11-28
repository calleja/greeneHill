#!/usr/bin/env python
# coding: utf-8

# In[8]:


#create a map of docker container credentials by system name
import platform

class Credentials:

    credentials_map = {
    'mofongo':{'user':'root','pass':'salmon01','database':'membership','port':3306,'host':'172.17.0.2'},
    'candela':{'user':'root','pass':'salmon01','database':'membership','port':3306,'host':'172.17.0.2'},
    'membership_ard':{'user':'lcalleja','pass':'salmon01','host':'100.102.223.21','port':3306,'database':'membership_ard'}
    }

    comp_name = platform.node()

    def get_credentils_map(self):
        return(Credentials.credentials_map)
    
    def retrieve_credentials(self,server_override = None):
        if server_override is None:
            if 'candela' in Credentials.comp_name:
                treated_name = 'candela'
        
            elif 'mofongo' in Credentials.comp_name:
                treated_name = 'mofongo'
            elif 'luis' in Credentials.comp_name:
                treated_name = 'luisito'    
        
            else:
                raise KeyError
        else:
            treated_name = server_override
        
        return Credentials.credentials_map[treated_name]
    

