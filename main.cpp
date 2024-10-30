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

string getLastFolderName(const string& path) {

    size_t lastSlashPos = path.rfind('/');

    if (lastSlashPos != std::string::npos) {
        return path.substr(lastSlashPos + 1);
    }
    
    return path;
}

void unlockTable(const string& pathToDir)
{
    
    string tableName = getLastFolderName(pathToDir);
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 0;
    lockFile.close();
}

void lockTable(const string& pathToDir)
{
    
    string tableName = getLastFolderName(pathToDir);
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 1;
    lockFile.close();
}

void increasePKSEQ(const string& tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

    string fileInput;
    ifstream PKSEQread(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQread.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQread, fileInput);
    PKSEQread.close();

    int increasedLinesAmount = stoi(fileInput) + 1;

    ofstream PKSEQupload(pathToDir + "/" + tableName + "_pk_sequence");
    PKSEQupload << increasedLinesAmount;
    PKSEQupload.close();
    
}

void decreasePKSEQ(const string& tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

    string fileInput;
    ifstream PKSEQread(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQread.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQread, fileInput);
    PKSEQread.close();

    int decreasedLinesAmount = stoi(fileInput) - 1;

    ofstream PKSEQupload(pathToDir + "/" + tableName + "_pk_sequence");
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
    string pathToDir = filesystem::current_path();
    pathToDir += dirName;
    if (filesystem::create_directory(pathToDir)) return true;
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
    string pathToDir = filesystem::current_path();
    pathToDir += subName;
    
    ofstream CSV(pathToDir + "/1.csv"); //Create and fill up .csv table
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

    string pathToDirPQ = pathToDir + "/" + table->string + "_pk_sequence";
    ofstream PKSEQ(pathToDirPQ); //Creating file-counter for each table
    PKSEQ << "1";
    PKSEQ.close();

    unlockTable(pathToDir);
}

void createDataBase()
{   
 
    LinkedList<string> tablePaths;
    string jsonContent = readJSON("schema.json"); //Reading json
    cJSON* json = cJSON_Parse(jsonContent.c_str()); //Parsing .json file
    cJSON* schemaName = cJSON_GetObjectItem(json, "name"); //Parsing DataBase name
    string DataBaseName = schemaName->valuestring;

    if (jsonContent.empty())
    {
        throw runtime_error("Error reading schema, content is empty!");
    }

    fstream checkDB("DataBaseFlag");
    if (checkDB.is_open())
    {
        string path = filesystem::current_path();
        filesystem::current_path(path + "/" + DataBaseName);
        checkDB.close();
        return;
    }

    ofstream DataBaseFlag("DataBaseFlag"); //Creating presence database flag
    DataBaseFlag.close();

    

    
    if (json == nullptr)
    {
        throw runtime_error("Error parsing schema file!");
    }

    cJSON* schemaLimit = cJSON_GetObjectItem(json, "tuples_limit"); //Parsing tuple limit
    int tuplesLimit = schemaLimit->valueint;

    
    
    createDir(DataBaseName); //Creating DataBase folder

    string path = filesystem::current_path();

    filesystem::current_path(path + "/" + DataBaseName);

    cJSON* structure = cJSON_GetObjectItem(json,"structure"); //Parsing structure

    for (cJSON* table = structure->child; table != nullptr; table = table->next) //Going through tables
    {
        string subFolderPath = filesystem::current_path();
        string tableName = table->string;
        subFolderPath =  "/" + tableName;
        tablePaths.addhead(subFolderPath);

        createDir(subFolderPath);

        createFilesInSubFolder(table, structure, subFolderPath);
    }
    cJSON_Delete(json);
}

int getPKSEQ(string tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

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

LinkedList<string> getColumnNamesFromTable(string tableName)
{
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;

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

LinkedList<HASHtable<string>> getTableLines(const string& tableName)
{
    
    LinkedList<HASHtable<string>> thisTable;
    int amountOfLinesInTable = getPKSEQ(tableName);
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    
    int filesCounter = ceil(static_cast<double>(amountOfLinesInTable)/1000); //Counting amount of .csv files
    for (int i = 0; i < filesCounter; i++)
    {
        int startRow = i * 1000;
        int endRow = min(startRow + 1000, amountOfLinesInTable);
        string fileInput;

        string pathToDir = filesystem::current_path();
        pathToDir += "/" + tableName;

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

LinkedList<HASHtable<string>> readTable(const string& tableName)
{
    LinkedList<HASHtable<string>> thisTable;
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;

    int amountLines = getPKSEQ(tableName);
    int fileCount = ceil(static_cast<double>(amountLines) / 1000);
    
    for (int i = 0; i < fileCount; ++i) //Creating CSV if >1000 elements
    {
        fstream fileCSV(pathToDir + "/" + to_string(i+1) + ".csv");
        if (!fileCSV.good())
        {
            ofstream newFile(pathToDir + "/" + to_string(i+1) + ".csv");
            newFile.flush();
            newFile.close();
        }
        fileCSV.flush();
        fileCSV.close();
    }

    thisTable = getTableLines(tableName);
    return thisTable;
}

void uploadTable(LinkedList<HASHtable<string>> table, string tableName)
{
    int linesAmount = getPKSEQ(tableName);
    int fileCount = ceil(static_cast<double>(linesAmount) / 1000);
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;
    for (int i = 0; i < fileCount; ++i) 
    {
        int startRow = i * 1000;
        int endRow = min(startRow + 1000, linesAmount);
        ofstream UPLOAD(pathToDir + "/" + to_string(i + 1) + ".csv", ios::out | ios::trunc);
        if (!UPLOAD.is_open()) throw runtime_error("Error opening csv for table upload");
        for (int row = startRow; row < endRow; row++)
        {
            for (int column = 0; column < columnNames.size(); column++)
            {
                auto currentRow = table.get(row);

                cout << currentRow.HGET(columnNames.get(column));

                if (column == columnNames.size() - 1)
                {
                    UPLOAD << currentRow.HGET(columnNames.get(column));
                    UPLOAD << "\n";
                }
                else
                {
                    UPLOAD << currentRow.HGET(columnNames.get(column));
                    UPLOAD << ",";
                }
                
            }
        }
        UPLOAD.flush();
        UPLOAD.close();
    }
    
}

void insert(LinkedList<string> values, string tableName)
{
    lockTable(tableName);
    LinkedList<HASHtable<string>> table = readTable(tableName);
    
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    HASHtable<string> row(columnNames.size());
    if (values.size() == columnNames.size())
    {   
        for (int i = 0; i < columnNames.size(); i++)
        {
            row.HSET(columnNames.get(i),values.get(i));
        }
        table.addtail(row);
        increasePKSEQ(tableName);
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
        increasePKSEQ(tableName);
    }
    else
    {
        unlockTable(tableName);
        throw runtime_error("Amount of values more than columns in table!");
    }

    uploadTable(table, tableName);

    unlockTable(tableName);
}

LinkedList<string> parseCommand(string userInput)
{
    LinkedList<string> dividedInput;
    string word = "";
    for (auto symbol : userInput)
    {
        if (symbol == '\'' || symbol == '(' || symbol == ')' || symbol == ' ' || symbol == ',')
        {
            if (!word.empty())
            {
                dividedInput.addtail(word);
            }
            word = "";
            continue;
        }
        word += symbol;
    }
    return dividedInput;
}

string parseTablenameForInsert(LinkedList<string> commandList)
{
    return commandList.get(2);
}

LinkedList<string> parseValuesForInsert(LinkedList<string> commandList)
{   
    LinkedList<string> values;
    if (commandList.get(3) != "VALUES")
    {
        throw runtime_error("Syntax error in pasing values for INSERT");
    }
    for (int i = 4; i < commandList.size(); i++)
    {
        values.addtail(commandList.get(i));
    }
    return values;
}

bool whereInside(LinkedList<string> commandList)
{
    return commandList.search("WHERE");
}

void handleInput(LinkedList<string> commandList)
{
    if (commandList.get(0) == "INSERT" && commandList.get(1) == "INTO" && whereInside(commandList) == 0)
    {
        LinkedList<string> values = parseValuesForInsert(commandList);
        string tableName = parseTablenameForInsert(commandList);
        insert(values, tableName);
    }
}

bool isTableName(const string& element)
{
    for (auto sym : element)
    {
        if(sym == '.') return true;
    }
    return false;
}

string divideAndGetTable(const string& word)
{
    string tableName = "";
    for (auto sym : word)
    {
        if (sym == '.') return tableName;

        else tableName += sym;
    }
    return "";
}   

string divideAndGetColumn(const string& word)
{
    bool writeMode = 0;
    string tableName = "";
    for (auto sym : word)
    {
        if (writeMode == 1)
        {
            tableName += sym;
        }
        if (sym == '.') writeMode = 1;
    }
    return tableName;
}

LinkedList<string> getSelectedTablesFROM(LinkedList<string> commandList)
{
    LinkedList<string> selected;
    
    bool writeMode = 0;
    for (int i = 0; i < commandList.size(); i++)
    {
        auto token = commandList.get(i);
        if (token == "WHERE") break;
        if ( writeMode == 1) selected.addtail(token);
        if (token == "FROM") writeMode = 1;

    }
    
    return selected;
}

LinkedList<string> getSelectedTablesSELECT(LinkedList<string> commandList)
{
    LinkedList<string> selected;
    
    bool writeMode = 0;
    for (int i = 0; i < commandList.size(); i++)
    {
        auto token = commandList.get(i);
        if (token == "FROM") break;
        if ( writeMode == 1) selected.addtail(token);
        if (token == "SELECT") writeMode = 1;

    }
    
    return selected;
}

LinkedList<bool> conditionCheckSELECT(LinkedList<string> conditions, string tableName, HASHtable<string> row) 
{
    LinkedList<bool> results;
    for (int i = 0 ; i < conditions.size(); i += 3)
    {
        string left = conditions.get(i);
        string op = conditions.get(i+1);
        string right = conditions.get(i+2);

        if (op == "=")
        {
            if (!isTableName(left)) throw runtime_error("SYNTAX error in condition check");
            if (conditions.search(tableName) == 0)
            {
                for (int j = 0; j < conditions.size(); j += 3)
                {
                    results.addtail(1);
                }
                return results;
            }
            else if (divideAndGetTable(left) == tableName)
            {
                bool check = row.HGET(divideAndGetColumn(left)) == right;
                results.addtail(check);
            }
            else if (divideAndGetTable(left) != tableName)
            {
                results.addtail(0);
            }
            
        }
        else
        {
            throw runtime_error("SYNTAX error in where condition");
        }
    }

    return results;
}

bool resultForDoubleSelect(LinkedList<string> operators, LinkedList<bool> firstRes, LinkedList<bool> secondRes)
{
    LinkedList<bool> resultingTwoList;
    for (int i = 0; i < firstRes.size(); i++)
    {
        resultingTwoList.addtail(firstRes.get(i) || secondRes.get(i));
    }

    bool finalRes;
    for (int i = 0; i < resultingTwoList.size() - 1; i++)
    {
        bool current = resultingTwoList.get(i);
        bool next = resultingTwoList.get(i + 1);
        if (i == 0) finalRes = current;
        else
        {   
            string op = operators.get(i-1);
            if (op == "AND") finalRes = current && next;
            else if (op == "OR") finalRes = current || next;
        }
    }
    return finalRes;
}

void handleConditionSELECT(LinkedList<string> commandList)
{
    LinkedList<string> conditions;
    LinkedList<string> operators;
    
    bool startWrite = 0;
    string element;
    for (int i = 0; i < commandList.size(); i++)
    {
        element = commandList.get(i);
        if (startWrite)
        {
            if (element == "OR" || element == "AND")
            {
                operators.addtail(element);
            }
            else
            {
                conditions.addtail(element);
            }
        }
        if (element == "WHERE") startWrite = 1;
        
    }
    

    LinkedList<string> selectedTables = getSelectedTablesFROM(commandList);
    if (selectedTables.size() == 2)
    {
        LinkedList<HASHtable<string>> goodTableRows1;
        LinkedList<HASHtable<string>> goodTableRows2;

        LinkedList<HASHtable<string>> firstTable = readTable(selectedTables.get(0));
        LinkedList<HASHtable<string>> secondTable = readTable(selectedTables.get(1));

        LinkedList<bool> resultOfCheckFirstTab;
        LinkedList<bool> resultOfCheckSecondTab;

        bool table1IsEmpty = false;
        bool table2IsEmpty = false;
        for (int i = 0; i < max(firstTable.size(), secondTable.size()); i++)
        {
            if (i < firstTable.size())
            {
                resultOfCheckFirstTab = conditionCheckSELECT(conditions, selectedTables.get(0),firstTable.get(i));
            }
            else table1IsEmpty = true;

            if (i < secondTable.size())
            {
                 resultOfCheckSecondTab = conditionCheckSELECT(conditions, selectedTables.get(1),secondTable.get(i));
            } 
            else table2IsEmpty = true;

            if (table1IsEmpty || table2IsEmpty)
            {
                if (table1IsEmpty) 
                {
                    resultOfCheckFirstTab = resultOfCheckSecondTab;
                }
                else if (table2IsEmpty)
                {
                    resultOfCheckSecondTab = resultOfCheckFirstTab;
                }
            }


            if (resultForDoubleSelect(operators,resultOfCheckFirstTab, resultOfCheckSecondTab) && (table1IsEmpty || table2IsEmpty) == 0)
            {
                goodTableRows1.addtail(firstTable.get(i));
                goodTableRows2.addtail(secondTable.get(i));
            }
            else
            {
                if (table1IsEmpty)
                {
                    goodTableRows2.addtail(secondTable.get(i));
                }
                else if (table2IsEmpty)
                {
                    goodTableRows1.addtail(firstTable.get(i));
                }
            }
        }

    LinkedList<string> selectedColumns = getSelectedTablesSELECT(commandList);
    for (int i = 0; i < goodTableRows1.size(); i++)
    {
        for (int j = 0; j < goodTableRows2.size(); j++)
        {
            cout << firstTable.get(i).HGET(divideAndGetColumn(selectedColumns.get(0)))   << " " << firstTable.get(i).HGET(divideAndGetColumn(selectedColumns.get(1)))  << endl;
        }
    }



    }
    else if (selectedTables.size() == 1)
    {
        LinkedList<string> columnNames = getColumnNamesFromTable(divideAndGetTable(selectedTables.get(0)));
        LinkedList<HASHtable<string>> goodTableRows1;
        LinkedList<bool> resultOfCheckFirstTab;
        LinkedList<HASHtable<string>> firstTable = readTable(selectedTables.get(0));
        LinkedList<string> selectedColumns = getSelectedTablesSELECT(commandList);
        if (selectedColumns.size() != 2) throw runtime_error("Wrong amount of columns selected");
        for (int i = 0; i < firstTable.size(); i++)
        {
            resultOfCheckFirstTab = conditionCheckSELECT(conditions, selectedTables.get(0),firstTable.get(i));

            cout <<  firstTable.get(i).HGET(divideAndGetColumn(selectedColumns.get(0)))   << " " << firstTable.get(i).HGET(divideAndGetColumn(selectedColumns.get(1)))  << endl;

        }
    } 
    else 
    {
        throw runtime_error("SYNTAX error in where wrong amount of tables used");
    }
    

   
}
 


int main()
{
    setlocale(LC_ALL, "RU");
    createDataBase();
    string userInput;
    getline(cin, userInput);
    LinkedList<string> inputList = parseCommand(userInput);
    system("clear");
    //inputList.print();


    handleConditionSELECT(inputList);

    // cout << inputList.size();
    // for (int i = 0; i < inputList.size(); i++)
    // {
    //     cout << inputList.get(i) << "|";
    // }

    



    
    //unlockTable("Схема 1/таблица1");
    //string tableName = "таблица1";
    
    

    // LinkedList<string> test3;
    // test3.addtail("POCHEMY");
    // test3.addtail("ONO");
    // test3.addtail("ROBIT");
    // test3.addtail("WOWOWOWOWOOWWO");
    
    // insert(test3, tableName);

    return 0;
}
