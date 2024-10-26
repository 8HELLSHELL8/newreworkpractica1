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
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 0;
    lockFile.close();
}

void lockTable(const string& pathToDir)
{
    //Current version takes "Схема 1/таблица1" as input
    string tableName = pathToDir.substr(13, tableName.size() - 8);
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 1;
    lockFile.close();
}

void increasePKSEQ(const string& pathToDir)
{
    string tableName = pathToDir.substr(13, tableName.size() - 8); //Getting table name

    string fileInput;
    ifstream PKSEQread(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQread.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQread, fileInput);
    PKSEQread.close();

    int increasedLinesAmount = stoi(fileInput) + 1;

    ofstream PKSEQupload(pathToDir + "/" + tableName + "_lock");
    PKSEQupload << increasedLinesAmount;
    PKSEQupload.close();
    
}

void decreasePKSEQ(const string& pathToDir)
{
    string tableName = pathToDir.substr(13, tableName.size() - 8); //Getting table name

    string fileInput;
    ifstream PKSEQread(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQread.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQread, fileInput);
    PKSEQread.close();

    int decreasedLinesAmount = stoi(fileInput) - 1;

    ofstream PKSEQupload(pathToDir + "/" + tableName + "_lock");
    PKSEQupload << decreasedLinesAmount;
    PKSEQupload.close();
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

int getPKSEQ(string pathToDir)
{
    string tableName = pathToDir.substr(13, tableName.size() - 8); //Getting table name

    string fileInput;
    ifstream PKSEQ(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQ.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQ, fileInput);
    PKSEQ.close();

    return stoi(fileInput);
}

LinkedList<string> getColumnNamesFromTable(string pathToDir)
{
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

    return columnNames;
}

LinkedList<HASHtable<string>> getTableLines(const string& pathToDir)
{
    
    LinkedList<HASHtable<string>> thisTable;
    int amountOfLinesInTable = getPKSEQ(pathToDir);
    LinkedList<string> columnNames = getColumnNamesFromTable(pathToDir);
    
    int filesCounter = ceil(static_cast<double>(amountOfLinesInTable)/1000); //Counting amount of .csv files
    for (int i = 0; i < filesCounter; i++)
    {
        int startRow = i * 1000;
        int endRow = min(startRow + 1000, amountOfLinesInTable);
        string fileInput;
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

LinkedList<HASHtable<string>> readTable(const string& pathToDir)
{
    LinkedList<HASHtable<string>> thisTable;

    int amountLines = getPKSEQ(pathToDir);
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

    thisTable = getTableLines(pathToDir);
    return thisTable;
}

void uploadTable(LinkedList<HASHtable<string>> table, string pathToDir)
{
    fstream UPLOAD()
}

void insert(LinkedList<string> values, string pathToDir)
{
    lockTable(pathToDir);
    LinkedList<HASHtable<string>> table = readTable(pathToDir);
    
    LinkedList<string> columnNames = getColumnNamesFromTable(pathToDir);
    HASHtable<string> row(columnNames.size());
    if (values.size() == columnNames.size())
    {   
        for (int i = 0; i < columnNames.size(); i++)
        {
            row.HSET(columnNames.get(i),values.get(i));
        }
        table.addtail(row);
        increasePKSEQ(pathToDir);
    }
    else if (values.size() < columnNames.size())
    {
        for (int i = 0; i < columnNames.size(); i++)
        {
            if (i >= values.size())
            {
                row.HSET(columnNames.get(i),"EMPTY");
            }
            else
            {
                row.HSET(columnNames.get(i),values.get(i));
            }
        }
        table.addtail(row);
        increasePKSEQ(pathToDir);
    }
    else
    {
        unlockTable(pathToDir);
        throw runtime_error("Amount of values more than columns in table!");
    }

    uploadTable(table, pathToDir);

    unlockTable(pathToDir);
}

int main()
{
    setlocale(LC_ALL, "RU");
    
    //createDataBase();
    //unlockTable("Схема 1/таблица1");
    LinkedList<string> test1;
    test1.addtail("1");
    test1.addtail("2");
    test1.addtail("3");
    test1.addtail("4");

    
    string pathToDir = "Схема 1/таблица1";
    insert(test1, pathToDir);


    

    return 0;
}
