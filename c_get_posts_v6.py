# Description:
#       Returns the Facebook posts information for a given set of pages
#
# Instructions:
#       Enter the name of the infile at the bottom of this script, under the section named "Execute" 
#           (infile location is marked by a triple pound sign ###)
#           Run the entire script when you set the parameters in the Execute section.
#       WARNING: CSV output files from the previous step may automatically turn the numeric facebook
#           ID #'s into scientific notation format instead of number format, and you will see a bunch
#           of zeroes towards the end of the ID. If this happens you will have to manually replace 
#           the notation with the actual ID #, otherwise the ID will not be found.
#           
#    
# Input:
#   a csv inputfile with a column labeled id.  column should contain facebook ids of pages from which you want posts
#   an output directory.  Results will be saved to that directory
#   a number indicating how many posts to return (default is 100)

#   Output:
#       Stores results for each page in a separate file.  Outputs csv and json by default

# Instructions:
#       Can be run from command line. eg. python  get_page_info.py -i [infile] -o [outdir] -m [number]
#       Can be run from command line interactively (it will prompt you for infile and outdir)
#       Can be run interactively by changing parameters below



# Import necessary modules
import requests
import pandas as pd
import os
import time
import datetime
import csv
import json
import pprint as pp
import math

# Command line getter -----------------------------
# Parse command line arguments and returns tupple containing input file and output directory
import sys, getopt
#def main(argv):     
#
#    """     
#    Takes command arguments (-i for input file and -o
#    for output location) and parses them.  If both parameters are not found, it
#    prompts the user to enter them.
#
#
#    Parameters
#    ----------
#    argv : 
#        array are arguments passed from command line
#
#    Returns
#    -------
#    result
#        a tuple containing the input and output values
#
#    Raises
#    ------
#    KeyError
#        when a error
#    OtherError
#        when an other error
#    """
#
#
#    inputfile = ''
#    outputfile = ''
#    maxposts = ''
#
#    #Parse command line options
#    try:
#      opts, args = getopt.getopt(argv,"hi:o:m:",["ifile=","ofile=", "maxpost"])
#    except getopt.GetoptError:
#      print 'file.py -i <inputfile> -o <outputfolder> -m <max number of posts>'
#      sys.exit(2)
#    for opt, arg in opts:
#      if opt in ("-h", "--help"):
#         print 'file.py -i <inputfile> -o <outputfolder> -m <max number of posts>'
#         sys.exit()
#      elif opt in ("-i", "--ifile"):
#         inputfile = arg
#      elif opt in ("-o", "--ofile"):
#         outputfile = arg
#      elif opt in ("-m", "--maxpost"):
#         outputfile = arg
#
#    if inputfile=="":
#            inputfile = raw_input("Please enter location of input file: ")
#
#    if outputfile=="":
#            outputfile = raw_input("Please enter location for output: ")
#
#    if maxposts=="":
#            maxposts = raw_input("Please enter maximum number of posts: ")
#
#    inputs = (inputfile, outputfile, maxposts)
#    return(inputs)



#if __name__ == "__main__":
#    inputs=  main(sys.argv[1:])



# Get page info for a particular URL ---------------------------------------------------------------
def queryFacebook(url,prevDat, id, resultpage, maxposts):
    r = requests.get(url)
    if r.status_code !=200:
        print("error getting" + url + "; code=" + str(r.status_code))
    else:
        response = r.json()
        #Add hmc rule, id, and current time to dictionary
        st = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        #response should always contain a data field that contains a list of posts
       
        #We loop over each post and add fields with metadata 
        if "data" in response:
            dat = response["data"]
            #print("number of posts is" + str(len(dat)))
            #print("dat" + str(type(dat)))
            for post in dat:
                post["HMC_queryDateTime"] = st
                post["HMC_timestamp"] = int(time.time())
                post["HMC_pageID"] = id
                post["HMC_resultpage"] =resultpage
        #append data to previous data
        result = prevDat + dat
        #print("results" + str(type(result)))
        #print("results" + str(len(result)))
        #if paging exists, then we need to loop through until no next:       
        if "paging" in response and "next" in response["paging"]:
            resultpage=resultpage+1
            #print("number of layers" + str(layer))
            print(int(maxposts)/100)
            if resultpage <= int(maxposts)/100:
                result = queryFacebook(response["paging"]["next"],result,id, resultpage, maxposts)
        return(result)
            
#used to initialize feed from a page.  This function triggers first page, then the queryFacebook function handles paging where applicable      
def getPageFeed(row, app_token, maxposts):
#commented out because it's returning "place" for some IDs but not others# url = "https://graph.facebook.com/v2.8/" + str(int(row['id'])) + "/feed?limit=100&fields=caption,description,id,link,message_tags,name,place,shares,parent_id,status_type,type,updated_time,message,created_time,from&access_token=" +  app_token
    url = "https://graph.facebook.com/v3.0/" + str(int(row['id'])) + "/feed?limit=100&fields=caption,description,id,link,message_tags,name,shares,parent_id,status_type,type,updated_time,message,created_time,from&access_token=" +  app_token
    dat=[]
    dat=queryFacebook(url, dat,row['id'],0, maxposts)
    
    #print("number of posts is " + str(len(dat)))
    return(dat)


# Get and save all page feeds ---------------------------------------------------------------------------------------
# Loops through pages, gets their feeds, and saves them out to specified directory
# if a limit is specified, only first x will be downloaded (useful for testing)
def getAllPageFeed(infile, outdir, app_token, limit=0, maxposts=3600, verbose=True):

    #load ids
    fbIDs = pd.read_csv(infile)

    #check limit
    if limit!=0:
        fbIDs = fbIDs[0:limit]

    #loop over
    counter=0
    for index, row in fbIDs.iterrows():
        counter+=1
        if counter %3==0:
            time.sleep(3600)
            if verbose==True:
             print("Now working: "  + str(counter) + " of " + str(len(fbIDs)) +"; " + str(row["id"]))
        fbIDs.set_value(index, "dateProcessed", int(time.time()))
        if not math.isnan(row["id"]):
            resultPage = getPageFeed(row, app_token, maxposts)
            if resultPage is not None:
                allPageResults = resultPage

                fname = os.path.join(outputPath, "FBPageFeed_" + str(row["id"])  +"_" + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d'))
                #fnamea = fname  + ".json"
                fnameb = fname  + ".csv"
                #with open (fnamea, "w") as fout:
                #   json.dump(allPageResults, fout, indent=4,)
                #to csv
                temp=pd.DataFrame(allPageResults)
                temp.to_csv(fnameb, encoding='utf-8', index=False, quoting= csv.QUOTE_ALL)
                allPageResults= []

    return(temp)

# Execute ----------------------------------------------------------------------------

#infile = inputs[0]
#outputPath =inputs[1]
#maxposts = inputs[2]
#note: this is a test token (removed for github)
token = "..."

infile="C:/Users/.../PageInfo/9_Extras.csv"
outputPath="C:/Users/.../Posts/"
maxposts=3600 # Change the actual maxposts on line 158: def getAllPageFeed(infile, outdir, app_token, limit=0, maxposts=5000, verbose=True):

print "start feed retriever: " + str(datetime.datetime.now())
df=getAllPageFeed(infile, outputPath, token, maxposts)
print ("end Feed Retriever: " + str(datetime.datetime.now()))


