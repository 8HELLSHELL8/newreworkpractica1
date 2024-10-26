//FOR Programm
#include <iostream>
#include "headers/LinkedList.h"
#include "headers/HashTable.h"
#include <cmath>
#include <string>

//FOR using files in system
#include <filesystem>

//FOR Parsing
#include <cjson/cJSON.h>
#include <fstream>
#include <sstream>

//FOR DataBase


using namespace std;

void unlockTable(const string& pathToDir)
{
    //Current version takes "Схема 1/таблица1" as input
    string tableName = pathToDir.substr(13, tableName.size() - 8);
    cout << pathToDir + "/" + tableName + "_lock";
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 0;
    lockFile.close();
}

void lockTable(const string& pathToDir)
{
    //Current version takes "Схема 1/таблица1" as input
    string tableName = pathToDir.substr(13, tableName.size() - 8);
    cout << pathToDir + "/" + tableName + "_lock";
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 1;
    lockFile.close();
}

string readJSON(const string& fileName) //Reading json content in string line
{ 
    fstream file(fileName);
    if (!file.is_open())
    {
        throw runtime_error("Error opening " + fileName + ".json file!");
    } 

    stringstream buffer;
    buffer << file.rdbuf();
    file.close();

    return buffer.str();
}

bool createDir(const string& dirName)
{
    if (filesystem::create_directory(dirName)) return true;
    else return false;
}

void createFilesInSubFolder(const cJSON* table, const cJSON* structure, const string& subName)
{
    LinkedList<string> columnNames; 
    cJSON* tableArray = cJSON_GetObjectItem(structure, table->string); //Reading table column names
    int arrSize = cJSON_GetArraySize(tableArray); //Reading amount of columns in table

    for (size_t i = 0; i < arrSize; i++) //Insert column names in table
    {
        cJSON* arrayItem = cJSON_GetArrayItem(tableArray, i);
        columnNames.addtail(arrayItem->valuestring);
    }

    ofstream CSV(subName + "/1.csv"); //Create and fill up .csv table
    for (size_t i = 0; i < columnNames.size(); i++)
    {   
        if (i < columnNames.size()-1)
        {
            CSV << columnNames.get(i) << ",";
        }
        else
        {
            CSV << columnNames.get(i);
        }
        
    }
    CSV << endl;
    CSV.close();


    ofstream PKSEQ(subName + "/" + table->string + "_pk_sequence"); //Creating file-counter for each table
    PKSEQ << "1";
    PKSEQ.close();

    ofstream PKLOCK(subName + "/" + table->string + "_lock"); //Creating a mutex for files
    PKLOCK << "0";
    PKLOCK.close();

}

void createDataBase()
{
    LinkedList<string> tablePaths;

    ofstream DataBaseFlag("DataBaseFlag"); //Creating presence database flag
    DataBaseFlag.close();

    string jsonContent = readJSON("schema.json"); //Reading json
    if (jsonContent.empty())
    {
        throw runtime_error("Error reading schema, content is empty!");
    }

    cJSON* json = cJSON_Parse(jsonContent.c_str()); //Parsing .json file
    if (json == nullptr)
    {
        throw runtime_error("Error parsing schema file!");
    }

    cJSON* schemaLimit = cJSON_GetObjectItem(json, "tuples_limit"); //Parsing tuple limit
    int tuplesLimit = schemaLimit->valueint;

    cJSON* schemaName = cJSON_GetObjectItem(json, "name"); //Parsing DataBase name
    string DataBaseName = schemaName->valuestring;

    createDir(DataBaseName); //Creating DataBase folder

    cJSON* structure = cJSON_GetObjectItem(json,"structure"); //Parsing structure

    for (cJSON* table = structure->child; table != nullptr; table = table->next) //Going through tables
    {
        string subFolderName = DataBaseName + "/" + table->string;
        tablePaths.addhead(subFolderName);

        createDir(subFolderName);

        createFilesInSubFolder(table, structure, subFolderName);
    }
}

LinkedList<HASHtable<string>> getTableLines(const string& pathToDir, int amountOfLinesInTable)
{
    
    LinkedList<HASHtable<string>> thisTable;

    ifstream COLNAMES(pathToDir + "/1.csv");
    string fileInput;
    getline(COLNAMES, fileInput, '\n');
    
    LinkedList<string> columnNames;
    string word = "";
    for (auto symbol : fileInput) //GETTING column names
    {
        if (symbol == ',')
        {
            columnNames.addtail(word);
            word = "";
            continue;
        }
        word += symbol;
    }
    if (!word.empty()) columnNames.addtail(word);
    COLNAMES.close();

    string tableName = pathToDir.substr(13, pathToDir.size() - 8); //Getting table name
    
    int filesCounter = ceil(static_cast<double>(amountOfLinesInTable)/1000); //Counting amount of .csv files
    for (int i = 0; i < filesCounter; i++)
    {
        int startRow = i * 1000;
        int endRow = min(startRow + 1000, amountOfLinesInTable);

        ifstream CSV(pathToDir + "/" + to_string(i+1) + ".csv");
        for (int row = startRow; row < endRow; row++)
        {
            getline(CSV, fileInput, '\n');
            HASHtable<string> tableLine(columnNames.size());
            string word = "";
            int wordCounter = 0;
            for (auto symbol : fileInput) //Process line
            {
                if (symbol == ',')
                {
                    tableLine.HSET(columnNames.get(wordCounter), word);
                    word = "";
                    wordCounter++;
                    continue;
                }
                word += symbol;
            }
            if (!word.empty()) tableLine.HSET(columnNames.get(wordCounter), word);
            thisTable.addtail(tableLine);
        }
        CSV.close();
    }
    
    return thisTable;
}

void readTable(const string& pathToDir)
{

    LinkedList<HASHtable<string>> thisTable;
    string tableName = pathToDir.substr(13, tableName.size() - 8); //Getting table name

    string fileInput;
    ifstream PKSEQ(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQ.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQ, fileInput);

    int amountLines = stoi(fileInput);
    int fileCount = ceil(static_cast<double>(amountLines) / 1000);
    
    for (int i = 0; i < fileCount; ++i) //Creating CSV if >1000 elements
    {
        fstream fileCSV(pathToDir + "/" + to_string(i+1) + ".csv");
        if (!fileCSV.good())
        {
            ofstream newFile(pathToDir + "/" + to_string(i+1) + ".csv");
            newFile.close();
        }
        fileCSV.close();
    }

    thisTable = getTableLines(pathToDir, amountLines);
    cout << "TABLE SIZE: " << thisTable.size() << "!!!!!" << endl;
    thisTable.get(0).print();

}



void insert(LinkedList<string> values, LinkedList<string> pathTodir)
{
    //LinkedList<HASHtable<string>> table = readTable();

}

int main()
{
    //setlocale(LC_ALL, "RU");
    
    //createDataBase();
    //unlockTable("Схема 1/таблица1");

    
    readTable("Схема 1/таблица1");

    return 0;
}
