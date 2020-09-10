#include <iostream>
#include <pqxx/pqxx>
#include <fstream>
#include <cstdlib>

#include "exerciser.h"

using namespace std;
using namespace pqxx;

void dropTable (connection *C, string name)
{
  stringstream stream;
  stream << "DROP TABLE IF EXISTS " << name << ";";
  transactionSQL(C, stream.str());
}

void dropExist (connection *C)
{
  dropTable(C, "PLAYER");
  dropTable(C, "TEAM");
  dropTable(C, "STATE");
  dropTable(C, "COLOR");

}

void createTable (connection *C)
{
  stringstream stream;
  /*   STATE   */
  stream << "CREATE TABLE STATE( ";
  stream << "STATE_ID   SERIAL   NOT NULL, ";
  stream << "NAME       TEXT     NOT NULL, ";
  stream << "PRIMARY KEY (STATE_ID) );";
  transactionSQL(C, stream.str());

  /*   COLOR   */
  stream.str("");
  stream << "CREATE TABLE COLOR( ";
  stream << "COLOR_ID   SERIAL   NOT NULL, ";
  stream << "NAME       TEXT     NOT NULL, ";
  stream << "PRIMARY KEY (COLOR_ID) );";
  transactionSQL(C, stream.str());
  
  /*   TEAM   */
  stream.str("");
  stream << "CREATE TABLE TEAM( ";
  stream << "TEAM_ID   SERIAL   NOT NULL, ";
  stream << "NAME      TEXT     NOT NULL, ";
  stream << "STATE_ID  INT      NOT NULL, ";
  stream << "COLOR_ID  INT      NOT NULL, ";
  stream << "WINS      INT      NOT NULL, ";
  stream << "LOSSES    INT      NOT NULL, ";
  stream << "PRIMARY KEY (TEAM_ID), ";
  stream << "CONSTRAINT TEAMSTATEFK FOREIGN KEY (STATE_ID)\
             REFERENCES STATE (STATE_ID) ON UPDATE CASCADE ON DELETE CASCADE,";
  stream << "CONSTRAINT TEAMCOLORFK FOREIGN KEY (COLOR_ID)\
             REFERENCES COLOR (COLOR_ID) ON UPDATE CASCADE ON DELETE CASCADE);";
  transactionSQL(C, stream.str());
  
  /*   PLAYER   */
  stream.str("");
  stream << "CREATE TABLE PLAYER( ";
  stream << "PLAYER_ID   SERIAL   NOT NULL, ";
  stream << "TEAM_ID     INT      NOT NULL, ";
  stream << "UNIFORM_NUM INT      NOT NULL, ";
  stream << "FIRST_NAME  TEXT     NOT NULL, ";
  stream << "LAST_NAME   TEXT     NOT NULL, ";
  stream << "MPG         INT      NOT NULL, ";
  stream << "PPG         INT      NOT NULL, ";
  stream << "RPG         INT      NOT NULL, ";
  stream << "APG         INT      NOT NULL, ";
  stream << "SPG DOUBLE PRECISION NOT NULL, ";
  stream << "BPG DOUBLE PRECISION NOT NULL, ";
  stream << "PRIMARY KEY (PLAYER_ID), ";
  stream << "CONSTRAINT PLAYERTEAMFK FOREIGN KEY (TEAM_ID)\
             REFERENCES TEAM (TEAM_ID) ON UPDATE CASCADE ON DELETE CASCADE);";
  transactionSQL(C, stream.str());
}

void readPlayer (connection *C)
{
  ifstream ifs("player.txt");
  string line, player_id, team_id, uniform_id, first_name, last_name, mpg, ppg, rpg, apg, spg, bpg;
  while (getline(ifs, line)){
    stringstream inputLine(line);
    inputLine >> player_id >> team_id >> uniform_id >> first_name >> last_name >> mpg >> ppg >> rpg >> apg >> spg >> bpg;
    add_player(C, atoi(team_id.c_str()), atoi(uniform_id.c_str()), first_name, last_name, atoi(mpg.c_str()), atoi(ppg.c_str()), atoi(rpg.c_str()), atoi(apg.c_str()), atof(spg.c_str()), atof(bpg.c_str()));
  }
  ifs.close();
}

void readTeam (connection *C)
{
  ifstream ifs("team.txt");
  string line, team_id, name, state_id, color_id, wins, losses;
  while (getline(ifs, line)){
    stringstream inputLine(line);
    inputLine >> team_id >> name >> state_id >> color_id >> wins >> losses;
    add_team(C, name, atoi(state_id.c_str()), atoi(color_id.c_str()), atoi(wins.c_str()), atoi(losses.c_str()));
  }
  ifs.close();
}

void readState (connection *C)
{
  ifstream ifs("state.txt");
  string line, state_id, name;
  while (getline(ifs, line)){
    stringstream inputLine(line);
    inputLine >> state_id >> name;
    add_state(C, name);
  }
  ifs.close();
}

void readColor (connection *C)
{
  ifstream ifs("color.txt");
  string line, color_id, name;
  while (getline(ifs, line)){
    stringstream inputLine(line);
    inputLine >> color_id >> name;
    add_color(C, name);
  }
  ifs.close();
}

void readFile (connection *C)
{
  readState(C);
  readColor(C);
  readTeam(C);
  readPlayer(C);
}

int main (int argc, char *argv[]) 
{

  //Allocate & initialize a Postgres connection object
  connection *C;

  try{
    //Establish a connection to the database
    //Parameters: database name, user name, user password
    C = new connection("dbname=ACC_BBALL user=postgres password=passw0rd");
    if (C->is_open()) {
      cout << "Opened database successfully: " << C->dbname() << endl;
    } else {
      cout << "Can't open database" << endl;
      return 1;
    }
  } catch (const std::exception &e){
    cerr << e.what() << std::endl;
    return 1;
  }


  //TODO: create PLAYER, TEAM, STATE, and COLOR tables in the ACC_BBALL database
  //      load each table with rows from the provided source txt files
  dropExist(C);
  createTable(C);
  //cout << "create tables" << endl;
  readFile(C);
  //cout << "read files" << endl;


  exercise(C);


  //Close database connection
  C->disconnect();

  return 0;
}


