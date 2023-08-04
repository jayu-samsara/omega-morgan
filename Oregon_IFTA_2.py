import requests
from datetime import datetime, timedelta
from datetime import datetime
import gzip
import pandas as pd
import csv
import requests
import sqlite3
import time
import json

# Establish Request Variables
# Omega Morgen
authorizon_bearer_token = "samsara_api_TpZOxeQpmS9MknztSlZbHhbLKIkzms"
url = "https://api.samsara.com/ifta-detail/csv"
current_datetime = datetime.now() + timedelta(hours=8)
print("current date",current_datetime)
#add timezone formatting. Currently missing!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
past_datetime = current_datetime - timedelta(hours=72)
rfc3339_now_format = current_datetime.isoformat("T") + "Z"
rfc3339_past_format = past_datetime.isoformat("T") + "Z"

print("now hour", rfc3339_now_format)
print ("past hour", rfc3339_past_format)

payload = {
    "endHour": rfc3339_now_format,
    "startHour": rfc3339_past_format
}
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "authorization": "Bearer "+authorizon_bearer_token
}


# Make Request for csv and Locate Job ID
response = requests.post(url, json=payload, headers=headers)
json_response = response.json()
print("json response",json_response)
jobId = json_response['data']['jobId']
print("job ID", jobId)





# Make Second Request for csv status
csv_status_url = "https://api.samsara.com/ifta-detail/csv/"+jobId

csv_status_headers = {
    "accept": "application/json",
    "authorization": "Bearer "+authorizon_bearer_token
}



jobStatus = "Processing"
while jobStatus != "Completed":
    print("Processing csv request")
    time.sleep(10)
    csv_status_response = requests.get(csv_status_url, headers=csv_status_headers)
    csv_status_response_json = csv_status_response.json()
    print(csv_status_response_json)
    jobStatus = csv_status_response_json['data']['jobStatus']
else: 
    print("URL retrieved")
    for x in csv_status_response_json['data']['files']:
        # Send a GET request to download the file
        downloadURL = x['downloadUrl']
        response = requests.get(downloadURL)

        # Check if the request was successful
        if response.status_code == 200:
            # Decompress the downloaded content
            decompressed_content = gzip.decompress(response.content).decode('utf-8')

            # Create a CSV reader
            reader = csv.reader(decompressed_content.splitlines(), delimiter=',')

            # Connect to the SQLite database
            conn = sqlite3.connect('/Users/jayu.patel/Downloads/oregonIftaCustomReport/Oregon_IFTA_try2.db') #SQL Location
            cursor = conn.cursor()

            # Execute a simple query to check the connection
            cursor.execute("SELECT SQLITE_VERSION();")
            version = cursor.fetchone()

            # Print the SQLite version to confirm the successful connection
            print("SQLite version:", version[0])
            

            # Iterate over each row in the CSV file
            for row in reader:
                # Check if the row corresponds to an Oregon segment
                if row[1] == 'CA':
                    # Extract relevant columns for Oregon segments
                    device_id = (row[0])
                    distance_meters = (row[2])
                    start_ms = (row[3])
                    end_ms = (row[8])
                    print("state", row[1])
                    # Insert the segment into the database
                    
                    cursor.execute("INSERT INTO mileage_segments (device_id, distance_meters, start_ms, end_ms) VALUES (?,?,?,?)",[device_id,distance_meters,start_ms,end_ms])
            # Commit the changes and close the database connection
            conn.commit()    

            print("Oregon Ifta segments imported successfully.")


        else:
            print("Failed to download the file.")







# Request all Oregon Weight Tax Documents
documentID = "4667572" #change document ID to org ID
hasNextPage = "true"
endCursor = ""

while hasNextPage == "true":
    documents_url = "https://api.samsara.com/fleet/documents?startTime="+rfc3339_past_format+"&endTime="+rfc3339_now_format+"&documentType="+documentID+"&after="+endCursor
    document_headers = {
        "accept": "application/json",
        "authorization": "Bearer "+authorizon_bearer_token
    }


    documents_response = requests.get(documents_url, headers=document_headers)
    documents_response_json = documents_response.json()
    print("Document Data",documents_response_json)
    documents_data = documents_response_json['data']

    if documents_data != None: 
        for x in documents_response_json['data']:    
            document_creation_timestamp = x['createdAtTime']
            document_vehicle_id = x['vehicle']['id']   
            document_vehicle_name = x['vehicle']['name']  
            document_fields = x['fields']                       
            print(document_creation_timestamp)
            print(document_vehicle_name) 

            for y in document_fields[0]['value']['multipleChoiceValue']:
                selected_true_or_false = y['selected']
                print("y",y)
                print(selected_true_or_false)
                if selected_true_or_false == True:
                    selected_value = y['value']
                    print("selected value",selected_value)
                    break

                    
            cursor.execute("INSERT INTO weight_tax_documents (vehicle_id, vehicle_name, creation_timestamp, weight_tax_value) VALUES (?,?,?,?)",[document_vehicle_id,document_vehicle_name,document_creation_timestamp,selected_value])
         
        # Commit the changes and close the database connection 
        print("Documents successfully imported")
        conn.commit()    
        conn.close()
    else:
        print("No documents to print")

    hasNextPage = documents_response_json['pagination']['hasNextPage']
    endCursor = documents_response_json['pagination']['endCursor']





def runReport():
    con = sqlite3.connect('/Users/jayu.patel/Downloads/oregonIftaCustomReport/Oregon_IFTA_try2.db') #change SQL link
    cur = con.cursor()
    mileageDatabase = cur.execute("SELECT device_id, distance_meters, start_ms, end_ms FROM mileage_segments;").fetchall()
    weightTaxDatabase = cur.execute("SELECT vehicle_id, vehicle_name, creation_timestamp, weight_tax_value FROM vweight_tax_doc_mileage_segments;").fetchall()
    con.close()
    resultsDataFrame = pd.DataFrame(columns=["Vehicle Name", "Vehicle ID", "Distance", "Document Submision Time", "Weight Tax Value"])
    meters = 0
    
    # itterate through the data from the databases
    for i in range(len(weightTaxDatabase)):
        curr_submition_time = timeConverter(weightTaxDatabase[i][2])
        curr_weight_tax = weightTaxDatabase[i][3] 
        curr_doc_deviceID = weightTaxDatabase[i][0]
        curr_name = weightTaxDatabase[i][1]
        counter = 0
        for j in range(len(mileageDatabase)):
            device_id, distance_meters = mileageDatabase[j][0], mileageDatabase[j][1]
            startTime, endTime = mileageDatabase[j][2], mileageDatabase[j][3]
            past_endTime = mileageDatabase[j-1][3]
            past_mileage_deviceId = mileageDatabase[counter][0]
            counter += 1
            if device_id == curr_doc_deviceID:
                if (device_id == past_mileage_deviceId):
                    if ((i == 0 or weightTaxDatabase[i-1][0] != curr_doc_deviceID) and int(endTime) <= curr_submition_time) or (i > 0 and int(endTime) >= timeConverter(weightTaxDatabase[i - 1][2]) and int(endTime) <= curr_submition_time):
                        meters += float(distance_meters)
                if (int(past_endTime) <= curr_submition_time and (int(startTime)) >= curr_submition_time):
                    resultsDataFrame.loc[len(resultsDataFrame)] = {"Vehicle Name": curr_name, "Vehicle ID": curr_doc_deviceID, "Distance": getMiles(int(meters)), "Document Submision Time": weightTaxDatabase[i][2], "Weight Tax Value": curr_weight_tax}
                    meters = 0
                    break
        if (meters != 0):
            print("end of segment")
            resultsDataFrame.loc[len(resultsDataFrame)] = {"Vehicle Name": curr_name, "Vehicle ID": curr_doc_deviceID, "Distance": getMiles(int(meters)), "Document Submision Time": weightTaxDatabase[i][2], "Weight Tax Value": curr_weight_tax}
            meters = 0
    print(resultsDataFrame)
    resultsDataFrame.to_csv(f"Oregan Ifta Tax Reporting {current_datetime}.csv", sep='\t', encoding='utf-8')
    
def timeConverter(timeStamp):
    test = datetime.strptime(timeStamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int(test.timestamp() * 1000)

def getMiles(meters):
    # *0.000621371192
    return meters
runReport()


        # next_submition_time = timeConverter(weightTaxDatabase[i+1][2])
        # next_name = weightTaxDatabase[i+1][1]
        # next_weight_tax = weightTaxDatabase[i+1][3] 
        # next_deviceID = weightTaxDatabase[i+1][0] 

          # elif device_id == curr_doc_deviceID and (int(startTime) <= curr_submition_time and (int(endTime) + 1203333) >= curr_submition_time) and past_weight_tax != curr_weight_tax:
            #     resultsDataFrame.loc[len(resultsDataFrame)] = {"Vehicle Name": curr_name, "Vehicle ID": curr_doc_deviceID, "Distance": getMiles(int(meters)), "Document Submision Time": weightTaxDatabase[i][2], "Weight Tax Value": curr_weight_tax}
            #     meters = 0
            # elif (int(past_endTime) <= curr_submition_time and (int(startTime)) >= curr_submition_time):
            #     resultsDataFrame.loc[len(resultsDataFrame)] = {"Vehicle Name": past_name, "Vehicle ID": past_deviceID, "Distance": getMiles(int(meters)), "Document Submision Time": weightTaxDatabase[i-1][2], "Weight Tax Value": past_weight_tax}
            #     meters = 0
            #     break
            # check to make sure that the row time is within the start and end time of the mileage database
                # add the meters whenever there is a change in the weight tax changes
                # weight is x at 8, at 12 weight changes, amount of distance between 8 and 12
                # adds the distance 
# Find the Driving Segmant within the mileage segment column and 
# assign the driving segmant with the Weight_Tax_Value if the weightTaxValue changes
# assuming that device ID == Vehicle ID

