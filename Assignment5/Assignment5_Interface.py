#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name:
#

from pymongo import MongoClient
import os
import sys
import json
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    business_in_City = collection.find({'city': {'$regex':cityToSearch, '$options':"$i"}})
    with open(saveLocation1, "w") as document:
        for business in business_in_City:
            displayBusiness(business, document)

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    business_in_Location = collection.find({'categories':{'$in': categoriesToSearch}}, {'name': 1, 'latitude': 1, 'longitude': 1, 'categories': 1})
    latitude1, longitude1 = float(myLocation[0]), float(myLocation[1])
    with open(saveLocation2, "w") as docs:
        for business in business_in_Location:
            displayBusinessDetails(business, maxDistance, docs, latitude1, longitude1)

def DistanceAlgorithm(latitude2, longitude2, latitude1, longitude1):
    R = 3959
    phi1, phi2 = math.radians(latitude1), math.radians(latitude2)
    change_pi = math.radians(latitude2-latitude1)
    change_lambda = math.radians(longitude2-longitude1)
    a = (math.sin(change_pi/2) * math.sin(change_pi/2)) + (math.cos(phi1) * math.cos(phi2) * math.sin(change_lambda/2) * math.sin(change_lambda/2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d

def displayBusiness(business, document):
    document.write(business['name'].upper() + "$" + business['full_address'].upper() + "$" + business['city'].upper() + "$" + business['state'].upper() + "\n")

def displayBusinessDetails(business, maxDistance, docs, latitude1, longitude1):
    latitude2 = float(business['latitude'])
    longitude2 = float(business['longitude'])
    distance = DistanceAlgorithm(latitude2, longitude2, latitude1, longitude1)
    if distance <= maxDistance:
        docs.write(business['name'].upper() + "\n")
