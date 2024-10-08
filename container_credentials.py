#!/usr/bin/env python
# coding: utf-8

# In[8]:


#create a map of docker container credentials by system name

def return_credentials():
    import platform 

    credentials_map = {
    'mofongo':{'user':'root','pass':'salmon01','database':'membership','port':3306,'host':'172.17.0.2'}
    }

    #retrieve computer name
    comp_name = platform.node()

    if 'mofongo' in comp_name:
        treated_name = 'mofongo'
    elif 'luis' in comp_name:
        treated_name = 'luisito'
    else:
        raise KeyError
    
    #return a dictionary
    return credentials_map[treated_name]
    

