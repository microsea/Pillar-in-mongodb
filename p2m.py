#!/usr/bin/python
"""
p2m.py is a script for import pillar data from .sls(salt stack file),
or directly from the command line input pillar's data to the mongodb.
"""
import argparse
#from pymongo import Connection
from pymongo import *
import salt,sys,yaml

parser = argparse.ArgumentParser(
    description='Import SaltStack pillar data to mongodb.',
    prefix_chars='-+/',
    prog='p2m.py',usage='%(prog)s [options]',
    )
group = parser.add_mutually_exclusive_group()
group.add_argument('-j','--json',action='store',default=False,
    dest='json_parser',help='Use json format data in command line to import.',type=str)
group.add_argument('-f','--file',action='store',default=False,
    dest='pillar_file',type=argparse.FileType('r'),help='Import pillar data from sls file.')
parser.add_argument('-M','--minion',action='store',default=False,
    dest='minionid',help='Minion id,like:10.5.117.200 or your_salt_minion_id.',type=str,required=True)
parser.add_argument('-N','--new',action='store',default=False,
    dest='new_pillar',help='Create new minion\'s pillar.Pls use double quotation marks.e.g. -N "{\'port\':\'9000\'}"',type=str,metavar='"{\'pillar_name\':\'pillar_value\'}"')
parser.add_argument('-U','--update',action='store',default=False,
    dest='update_pillar',help='Modify a pillar item,use -F to force update.Pls use double quotation marks.e.g. -U "{\'type\':\'nginx\'}"',type=str,metavar='"{\'pillar_name\':\'pillar_value\'}"')
parser.add_argument('-D','--delete',action='store',default=False,
    dest='delete_pillar',help='Delete a pillar item.Script will prompt delete or not,only if use -F to force delete item')
parser.add_argument('-S','--search',action='store',default=False,
    dest='search_pillar',help='Return a json pillar data.',type=str,metavar='pillar_name')
parser.add_argument('-s','--salt',action='store',default=False,
    dest='saltkey',help='Use salt commmand check pillar data.',type=str,metavar='pillar item')
parser.add_argument('-F','--force',action='store_true',default=False,
    dest='forceaction',help='Force update/delete pillar item.')


parser.add_argument(
    '--version',
    action='version',
    version='%(prog)s 1.0')

args = parser.parse_args()

#get content from CLI
get_json_data=args.json_parser
minionid=args.minionid
keyword=args.search_pillar
deleteit=args.delete_pillar
newpillar=args.new_pillar
uppillar=args.update_pillar
forceaction=args.forceaction
saltkey=args.saltkey
pillarfile=args.pillar_file

#connect to mongodb
databaseName = "pillar_mongodb"
connection = Connection('10.5.117.220', 27017)
db = connection[databaseName]
pillar=db['pillar']

#check argument json data in mongodb
def check_exists(id,key):
    idfind=pillar.find(id)
    if idfind.count() >= 1:
        #mongodb find : pillar.find({$and:[%s,{'%s':{$exists:true}}]}) % (id,key)
        #id type is dict: id['_id']=minionid
        #key type is str: mongodb.version
        fstr={}
        kexit={}
        kexit[key]={'$exists':'true'}
        kcon=[id,kexit]
        fstr['$and']=kcon
        keyfind=list(pillar.find(fstr))
        if keyfind:
            #print "found: %s" % keyfind
            return keyfind
        else:
            return False
    else:
        return False

#Delete pillar item,e.g. '-D mongodb.port'
def delete_pillar(id,key):
    #idfind=pillar.find(id)
    idfind=search_pillar(id,key)
    if idfind:
        #mongodb delete : pillar.update({$and:[%s,{$unset:{'%s':''}]} % (id,key)
        #print "pillar.update({'_id':'%s'},{$unset:{'%s':''}})" % (id,key)
        yn = raw_input("Are you sure delete %s's pillar item: \"%s\"(Y/N) " % (id['_id'],idfind))
        if yn == 'Y':
            fstr={}
            delitem={}
            delitem['$unset']={key:''}
            keydel=pillar.update(id,delitem)
            ifdel=check_exists(id,key)
            if ifdel:
                print "Faild: Found pillar item: \"%s:%s\" Delete false,pls check it." % (key,ifdel)
                return False
            else:
                print "Success: Deleted %s 's pillar item: %s" % (id['_id'],key)
                return True
        else:
            print "Give up delete action."
    else:
        print "Faild: No minionid found:%s" % id
        return False
#Create a minion pillar tree,only the minion has no pillar
def create_pillar(id,new):
    idfind=pillar.find(id)
    if idfind.count() >= 1:
        #print "%s exists: %s" % (id['_id'],idfind)
        return False
    else:
        newdata=dict(id.items()+eval(new).items())
        checknew=pillar.insert(newdata)
        #print "newdata: %s" % newdata
        if checknew:
            return True
        else:
            return False

#Update pillar data
def update_pillar(id,key):
    if key:
        upitem={}
        upitem['$set']=key
        pillar.update(id,upitem,upsert=True)
        print "Success: Updated id: %s 's pillar data: %s,pls check it." % (id,key)
    else:
        print "Fiald: Key is null,please check it."
        
#get all pillar's item,put into a list    
def dictkeylist(data,fkey,keylist):
    if isinstance(data,dict) :
        for x in range(len(data)):
            temp_key = data.keys()[x]
            temp_value = data[temp_key]
            if fkey:
                temp_key=fkey+'.'+temp_key
            #print "%s.%s : %s" %(fkey,temp_key,temp_value)
            if isinstance(temp_value,dict) :
                dictkeylist(temp_value,temp_key,keylist) #call itself to get all key
            else:
                keylist.append(temp_key)
    return keylist
#make a multiple dict into 2 level dict
#Exp: {'aa':{'bb':'cc','dd':'ee'}} ==> {'aa.bb': 'cc', 'aa.dd': 'ee'} 
def simpledict(data,keys,lastdict):
    if isinstance(data,dict):
        for x in range(len(data)):
            temp_key = data.keys()[x]
            temp_value = data[temp_key]
            if keys:
                temp_key=keys+'.'+temp_key
            if isinstance(temp_value,dict) :
                tmp_tmp_value=simpledict(temp_value,temp_key,lastdict) #call itself to get all key
                #lastdict= lastdict.items() + tmp_tmp_value.items()
            else:
                lastdict[temp_key]=temp_value
        return lastdict 
###### For test simpledict ###########
#aaa={'aa':{'bb':'cc','dd':'ee'}}
#bb=''
#cc={}
#simp=simpledict(aaa,bb,cc)
#print simp
#exit()
######################################
def search_pillar(idkey,keyword):
    search_result=check_exists(idkey,keyword)
    if search_result:
        for result in search_result:
            key_list=dictkeylist(result,'',keylist)
            #print key_list
            if result:
                result_simple=simpledict(result,'',tmpdict)
                #print "Minion id pillar data in mongodb is: %s" % result_simple
                for keytmp in key_list:
                    if keyword == keytmp:
                        #print "%s value is : %s" % (keyword,result_simple[keytmp])
                        return result_simple[keytmp] 
    else:
        return False

############# MAIN ########################
#init pillar data
pillar_data={}
idkey={}
tmpdict={}
keylist=[]
idkey['_id']=minionid
####################
#search pillar
####################
if keyword:
    result=search_pillar(idkey,keyword)
    if result:
        print result
    else:
        print "Faild: No found your pillar item %s in mongodb." % keyword
        
####################
#delete pillar
####################
if deleteit:
    delete_pillar(idkey,deleteit)

####################
#New pillar tree
####################
if newpillar:
    new_pillar=create_pillar(idkey,newpillar)
    if new_pillar:
        print "Success: Created minion: %s 's pillar tree." % idkey['_id']
    else:
        print "Faild: Minion id already exists,pls use -U to insert or update pillar data"

####################
#Update pillar
####################
if uppillar:
    uppillar_data=eval(uppillar)
    if forceaction:
        update_pillar(idkey,uppillar_data)
    else:
        for keytmp in uppillar_data.keys():
            #chk=check_exists(idkey,keytmp)
            chk=search_pillar(idkey,keytmp)
            if chk:
                print "Warning: Found pillar data in mongodb: %s.\nIf you want to update automatic,please use -F|--force to force update it. " % chk
                yn = raw_input("Are you sure update %s's pillar item: \"%s\"(Y/N)? " % (idkey,keytmp))
                if yn =='Y':
                    update_pillar(idkey,uppillar_data)
                else:
                    print "Give up update pillar data."
            else:
                print "Warning: No pillar item %s found,nothing to update" % keytmp
                yn = raw_input("Do you want to insert this pillar item: \"%s\"(Y/N)? " % keytmp)
                if yn =='Y':
                    update_pillar(idkey,uppillar_data)
                else:
                    print "No action taken."
    
####################
# import yaml file
####################
if pillarfile:
    readdata=yaml.load(pillarfile)
    if readdata:
        update_pillar(idkey,readdata)
        print "Success: Finish import action,pls check it."
    else:
        print "Faild: No data in pillar file!"
    print readdata
    
####################
#check salt pillar 
####################
if saltkey:
    salt = salt.client.LocalClient()
    #saltcmd="pillar.items %s" % saltkey
    pillaritem=[saltkey]
    pillars=salt.cmd(minionid ,'pillar.item' ,pillaritem,timeout=20)
    #print pillars
exit()
#if get_json_data:
#    pillar_data=dict(idkey.items()+eval(get_json_data).items())
    #print "data: %s" % eval(get_json_data)
#    the_key_list=dictkeylist(eval(get_json_data),'',keylist)
    #print the_key_list
    #pillar.update(idkey,pillar_data)

#for i in range(len(keylist)):
#    thekey=keylist[i]
#    exists=check_exists(idkey,thekey)
#    if exists:
#        for item in exists:
#            print "found exists pillar data: {%s:%s}" % (thekey,item)
#            exit(0)
#    else:
#        print "No found key: %s" % thekey


