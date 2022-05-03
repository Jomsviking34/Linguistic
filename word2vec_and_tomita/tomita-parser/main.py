import json
import os
import subprocess as sub
import sys

from pymongo import MongoClient

_id = 0
_id2 = 0
page_num = 0
                                                                  
try:
    print('conected..')

    client = MongoClient('localhost', 27017)   # подключение к БД
    db = client["News"]                                           
    collection = db.collection                                    
    tonality = db['TomitaCollection']                             
    articlesDB = db['ArticlesCollection']                         
                                                                                                                        
    tomitaDel = db.newsTomitaDB.delete_many({})
    articleDel = db.ArticlesDB.delete_many({})
    print(tomitaDel.deleted_count, "tonality deleted!")
    print(articleDel.deleted_count, "articles deleted!")

except:
    print('error')

# сохранение объектов в БД
def facts_for_reload(name, array: list, text):              
    factInside = False                                      
    content = ''
#Проход по массиву
    for object in array:
        if object == name:                                  
            factInside = True                               
        elif object == "{":                                 
            continue
        elif object == "}":
            factInside = False                              
        else:                                               
            position = object.find("=")                     
            if position > 0:                                
                left, right = object.split(" = ")           
            if position <= 0:                               
                content = object                            
                content = content.lower()                   
            if factInside and content:                      
                right = right.lower()                       
                rightnew = right.replace(' ', '_')          
                rightnew = rightnew.replace('\"', '')       
                content = content.replace(right, rightnew)  
                if name == 'Person':                        
                    DBSaveFunc(rightnew, '', content, text) 
                if name == 'Place':                         
                    DBSaveFunc('', rightnew, content, text) 
                text = text.lower()                         
                text = text.replace(right, rightnew)        
                DBSaveArticle(text)                         


def DBSaveFunc(person, place, content, text):                                                              
    global _id                                                                                             
    _id = _id + 1                                                                                          
    data = {'Person': person, 'Place': place, 'Sentence': content, 'Text': text, 'originalID': article_id} 
    print(data)                                                                                            
    tonality.update_one({"Id": _id}, {"$set": data}, upsert=True)                                          
    data2 = {'people': person, 'places': place}                                                            
    collection.update_one({"_id": _id2+1}, {"$set": data2}, upsert=True)                                   


def DBSaveArticle(text):                                                
    global _id2                                                         
    _id2 = _id2 + 1                                                     
    data = {'Текст': text}                                              
    articlesDB.update_one({"Id": _id2}, {"$set": data}, upsert=True)    



def mainFunc(records: list):                           
    with open('textForCourseProject.txt', 'w') as log: 
        appending = False                              
        person = ''                                    
        place = ''                                     
        for record in records:                         
            global page_num                            
            global article_id                          
            page_num = page_num + 1                    
            print('обработка статьи: ' + str(page_num))
            text = record['text']                      
            article_id = record['_id']                 
            out = TomitaGO(text)                       
            for obj in out:                            
                log.write("%s\n" % obj)                
            if "Person" in out:                        
                appending = True                       
                print('success for person')            
                facts_for_reload('Person', out, text)  
                print(person)                          
                print('\n')                            
            elif "Place" in out:                       
                appending = True                       
                print('success for place')             
                facts_for_reload('Place', out, text)   
                print(place)                           
                print('\n')                            
            else:                                      
                DBSaveArticle(text)                    
    log.close()                                        


def TomitaGO(txt):         
    os.chdir('/home/goblin/tomita-parser/build/bin')
    with open('input.txt', 'w', encoding='utf-8') as inputFile:  
        inputFile.writelines(txt)                                
    output = []                                                  
    os.system("./tomita-parser config.proto")                    
    with open('output.txt', 'r',                                 
              encoding='utf-8') as outputFile:                   
        buffer = ''                                              
        whatNew = outputFile.readlines()                         
        appending = False                                        
        savebuffer = True                                        
        for line in whatNew:                                     
            if "Person" in line or "Place" in line:              
                savebuffer = False                               
                appending = True                                 
                buffer = takeSomeInEnd(buffer)                   
                output.append(buffer.strip())                    
            if appending:                                        
                output.append(line.strip())                      
            if savebuffer:                                       
                buffer = buffer + line                           
            if "}" in line:                                      
                appending = False                                
                buffer = ' '                                     
                savebuffer = True                                
                                                                 
    return output                                                


def SplitContent(content):                      
    import re                                   
    result = re.split("\. |\.\.\. ", content)   
    return result                               


def takeSomeInEnd(txt):                 
    content = SplitContent(txt)         
    i = 0                               
    for _ in content:                   
        i = i + 1                       
        if i == 1:                      
            last_element = content[-1]  
        elif i == 0:                    
            last_element = " "          
        else:                           
            last_element = content[-2]  
    return last_element                 


def getDataDBs(mongo):              
    return mongo.selectAll("news")  


if __name__ == '__main__':
    Records = collection.find() 
    mainFunc(Records)           
