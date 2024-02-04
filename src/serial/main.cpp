/* 
 * tcpserver.c - A multithreaded TCP echo server 
 * usage: tcpserver <port>
 * 
 * Testing : 
 * nc localhost <port> < input.txt
 */
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sstream>
#include <arpa/inet.h>
#include <regex>

using namespace std;


#define maxCon 10
#define SOCKERROR (-1)
#define LOGGING 0
#define BUFSIZE 1024
#define THREADPOOL 20

typedef struct sockaddr_in SA_IN;
typedef struct sockaddr SA;

char client_message[1024];
map<string,string> KV_DATASTORE;

int check(int code , const char* msg);
void *handle_connection(void *p_client_socket);

string removeWS(string &str);
string write(string key , string value);
string read (string key);
string count();
string remove(string key);

int main(int argc, char ** argv) {
  int portno; /* port to listen on */
  
  /* 
   * check command line arguments 
   */
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  portno = atoi(argv[1]);

  SA_IN server_addr , client_addr ;
  memset(&server_addr,0,sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	server_addr.sin_port = htons(portno);

  int server_sock = check(socket(AF_INET, SOCK_STREAM, 0),"Socket faild to exist");
   
	//bind the socket to its local address
  int bindStatus = check(bind(server_sock, (SA *) &server_addr, sizeof(server_addr)),"Error binding socket to local address");

  #ifdef LOGGING
    cout << "Waiting for a client to connect..." << endl;
  #endif // LOGGING
	
	check(listen(server_sock, maxCon),"Listening failed");

  while(true)
  {
    #ifdef LOGGING
        cout << "Waiting for connections..."<<endl;
    #endif // LOGGING
    
    int client_sock;
    int addr_size = sizeof(SA_IN);
    check(client_sock = accept(server_sock,(SA *)&client_addr,(socklen_t *)&addr_size),"accept failed");
    
    #ifdef LOGGING
        cout << "connected to client socket "<<client_sock<<endl;
    #endif
    
    pthread_t thread;
    int *pclient = (int *)malloc(sizeof(int));
    *pclient = client_sock;
    handle_connection(pclient);

  }
  close(server_sock);
  #ifdef LOGGING
    cout << "server socket finished " << server_sock << endl;
  #endif
  return 0;
}

int check(int code , const char *msg)
{
  if(code == SOCKERROR){
    perror(msg);
    exit(0);
  }
  return code;
}

void *handle_connection(void *p_client_socket)
{
  #ifdef LOGGING
    cout << "In handle connection" <<endl;
  #endif
  int client_sock = *((int *)p_client_socket);
  free(p_client_socket);
  recv(client_sock, client_message, 1024, 0);

  string inputString(client_message);
  istringstream iss(inputString);
  string line;
  

  while(getline(iss >> ws,line))
  {   
    line = removeWS(line);
    if (line == "DELETE") 
    {   
        string key;
        getline(iss >> ws,key);
        key = removeWS(key);
        #ifdef LOGGING
            cout << "Performing DELETE operation of key = "<<key<< endl;
        #endif
        const char* done = remove(key).c_str();
        write(client_sock,done,strlen(done));

    } 
    else if (line == "COUNT") 
    {
        #ifdef LOGGING
            cout << "Performing COUNT operation. Number of KV pairs: " << endl;
        #endif
        const char* count_value = count().c_str();
        write(client_sock,count_value,strlen(count_value));
    } 
    else if (line == "READ") 
    {
        string key;
        
        getline(iss >> ws,key);
        key = removeWS(key);
        #ifdef LOGGING
            cout << "Performing READ operation for key: " << key << endl;
        #endif
        const char* value_read = read(key).c_str();
        write(client_sock,value_read,strlen(value_read));

    } else if (line == "WRITE") {
        string key, value;
        getline(iss >> ws, key);
        key = removeWS(key);
        getline(iss >> ws, value);
        value = removeWS(value);
        #ifdef LOGGING
            cout << "Performing WRITE operation. Key: " << key << ", Value: " << value << endl;
        #endif
        const char* value_wrote = write(key,value).c_str();
        write(client_sock,value_wrote,strlen(value_wrote));


    } else if (line == "END") {
        #ifdef LOGGING
            cout << "Ending the connection" << endl;
        #endif
        close(client_sock);
        break;  // Exit the loop upon encountering END
    } else {
        #ifdef LOGGING
            cout << "Unknown command: " << line << endl;
        #endif
    }

  }

  return NULL;
}


string removeWS(string &str)
{
    static regex whiteSpace("\\s+");
    str = regex_replace(str,whiteSpace,"");
    return str;
}

string write(string key , string value)
{   
    static regex removeColon(":");
    value = regex_replace(value,removeColon,"");
    KV_DATASTORE[key] = value;
    return "FIN\n";

}

string read (string key) {
    
    auto it = KV_DATASTORE.find(key);
    
    if(it != KV_DATASTORE.end())
    {
        return it->second+"\n";
    }
    return "NULL\n";
}

string count()
{
    return to_string(KV_DATASTORE.size())+"\n";
}

string remove(string key)
{   
    auto it = KV_DATASTORE.find(key);
    if( it != KV_DATASTORE.end())
    {
        KV_DATASTORE.erase(it);
        return "FIN\n";
    }
    return "NULL\n";
}