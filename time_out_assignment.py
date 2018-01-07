#!/usr/bin/python

from __future__ import unicode_literals
from pyspark.sql import SparkSession,DataFrame
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import size
from pyspark.sql.functions import array, lit
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import lower, col,udf
from pyspark.sql.types import StructType
from functools import reduce
import sys 


spark = SparkSession\
    .builder\
    .appName("example-spark")\
    .config("spark.sql.crossJoin.enabled","true")\
    .getOrCreate()
sc = spark.sparkContext

venues = spark.read.load("venues.json", format="json", multiLine=True)
users = spark.read.load("users.json", format="json", multiLine=True)

def getListOfTeamMembers():
    team_members = []
    try_again = False
    start = 0
    while try_again or start == 0:
        text = input("Enter list of names separated by comma or '1' to exit: ")
    
        if not str(text):
            print("You entered an empty value")
            try_again = True
        
        else:
            
            if str(text).strip() == '1':
                print("Thank you for using the app")
                break
            else:
                print("You entered " + str(text))
                names = str(text).split(",")
                for name in names:
                    team_members.append(name.lower().title().strip())
                if len(team_members)>0:
                    print("{} Team members going out".format(len(team_members)))
                    print("-------------------------")
                    for name in team_members:
                        print(name)
            try_again = False
        start +=1
        
        return team_members
    
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def filterByFood(users,venues):
    empty_return = None
    names = getListOfTeamMembers()
    if names:
        usersFiltered = users.filter(users.name.isin(names))
        
        if usersFiltered.count() > 0:
            wont_eat_foods = usersFiltered.select(usersFiltered.wont_eat,usersFiltered.name).rdd.map(lambda x: [x[0],x[1]]).collect()
    
            count = True
            for foods in wont_eat_foods:
        
                if count:
                    venuesFoodFiltered = venues
                    venuesFoodRemoved = venues.filter(venues.food == array(*[lit(x) for x in foods[0]]))
                    venuesFoodRemoved = venuesFoodRemoved.withColumn('user',lit(foods[1]))
                    count =False
    
                venuesFoodRemovedTemp = venues.filter(venues.food == array(*[lit(x) for x in foods[0]]))
                venuesFoodRemovedTemp = venuesFoodRemovedTemp.withColumn('user',lit(foods[1]))
                venuesFoodRemoved = unionAll(venuesFoodRemoved, venuesFoodRemovedTemp)  
                venuesFoodFiltered = venuesFoodFiltered.filter(venuesFoodFiltered.food != array(*[lit(x) for x in foods[0]]))  
    
            lower_udf = udf(lambda x: [str(drink).lower() for drink in x])
            venuesFoodFiltered = venuesFoodFiltered.withColumn("drinks", lower_udf(col("drinks")))
            venuesFoodRemoved = venuesFoodRemoved.withColumn("drinks", lower_udf(col("drinks"))).withColumn("removedDueTo",lit("eat"))
            venuesFoodRemoved = venuesFoodRemoved.dropDuplicates()
    
            return usersFiltered,venuesFoodFiltered,venuesFoodRemoved
        else:
            print('User Name(s) not found')
            return empty_return,empty_return,empty_return
    else:
        
        return empty_return,empty_return,empty_return
       
def filterByDrink(venuesFoodFiltered,usersFiltered):
    allDrinks = usersFiltered.select(usersFiltered.drinks,usersFiltered.name).rdd.map(lambda x: [x[0],x[1]]).collect()    
    count = True
    countAll =0 
    for drinks in allDrinks:
        #print("1. {}".format(drinks)) 
        inner_count = True
        for drink in drinks[0]:
            #print("2. {} {}".format(drink,len(drinks[0])))
            if count:
            
                venuesDrinkFilteredAll = venuesFoodFiltered.where(venuesFoodFiltered.drinks.like('%{}%'.format(drink.lower())))
                venuesDrinkFilteredAll = venuesDrinkFilteredAll.withColumn('user',lit(drinks[1]))
                venuesDrinkNonFiltered = venuesFoodFiltered.withColumn('user',lit(drinks[1]))
                count = False     
            else: 
                venuesDrinkFiltered = venuesFoodFiltered.where(venuesFoodFiltered.drinks.like('%{}%'.format(drink.lower())))
                venuesDrinkRemovedTemp = venuesDrinkFiltered.withColumn('user',lit(drinks[1]))
                venuesDrinkFilteredAll = unionAll(venuesDrinkFilteredAll, venuesDrinkRemovedTemp) 
                venuesDrinkFilteredAll = venuesDrinkFilteredAll.dropDuplicates()
            if inner_count and countAll > 0:
            
                venuesDrinkNonFilteredTemp = venuesFoodFiltered.withColumn('user',lit(drinks[1]))
                venuesDrinkNonFiltered = unionAll(venuesDrinkNonFiltered, venuesDrinkNonFilteredTemp)
                inner_count = False
        countAll +=1
            
    venuesDrinkRemoved = venuesDrinkNonFiltered.select('drinks','food','name','user').subtract(venuesDrinkFilteredAll.select('drinks','food','name','user'))           
    venuesDrinkRemoved = venuesDrinkRemoved.withColumn("removedDueTo",lit("drink"))  
    extraVenuesNames = [str(x.name) for x in venuesDrinkRemoved.select(venuesDrinkRemoved.name).collect()]
    venuesDrinkFilteredAll = venuesDrinkFilteredAll.filter(~venuesDrinkFilteredAll.name.isin(extraVenuesNames))
    
    
    return venuesDrinkFilteredAll,venuesDrinkRemoved

blackBulletPoint = u'\U00002022'
whiteBulletPoint = u'\U000025E6'
usersFiltered,venuesFoodFiltered,venuesFoodRemoved = filterByFood(users,venues)
if venuesFoodFiltered and venuesFoodRemoved:
    
    venuesDrinkFilteredAll,venuesDrinkRemoved = filterByDrink(venuesFoodFiltered,usersFiltered)

    removedVenues = unionAll(venuesFoodRemoved, venuesDrinkRemoved)
    placesToGo = [name for name in venuesDrinkFilteredAll.drop("user").dropDuplicates().select(venuesDrinkFilteredAll.name).rdd.flatMap(lambda x: x).collect()]
    PlacesToAvoid = removedVenues.select(removedVenues.name,removedVenues.user,removedVenues.removedDueTo).rdd.map(lambda x: (x[0],x[1],x[2])).collect()
 
    print("\n Places to go \n")
    for name in placesToGo :
        print("   {} {} ".format(blackBulletPoint,name))
    
    print("\n Places to avoid \n")
    previousName = None

    for row in PlacesToAvoid :
        if previousName is None or (previousName != row[0]):
        
            print("   {} {} ".format(blackBulletPoint,row[0]))
            print("     {}  There is nothing for {} to {}".format(whiteBulletPoint,row[1],row[2]))
        elif (previousName == row[0]):
            print("     {}  There is nothing for {} to {}".format(whiteBulletPoint,row[1],row[2]))
        previousName = row[0]
        