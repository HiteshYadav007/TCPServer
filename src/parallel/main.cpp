#include<iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <pthread.h>
#include <sstream>
#include <sys/socket.h>
#include <regex>
#include <unistd.h>
#include <arpa/inet.h>
using namespace std;

#define maxCon 100
#define SOCKERROR (-1)
#define BUFSIZE 1024
#define THREADPOOL 20
#define LOGGING 0

typedef struct sockaddr_in SA_IN;
typedef struct sockaddr SA;

pthread_t thread_pool[THREADPOOL];
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_var = PTHREAD_COND_INITIALIZER;

char client_message[1024];
map<string,string> KV_DATASTORE;

int check(int code , const char* msg);
void *handle_connection(void *p_client_socket);
void *thread_function(void *arg);

struct node
{
    struct node *next;
    int *client_socket;
};
typedef struct node node_t;

void enqueue(int *client_socket);
int* dequeue();

string removeWS(string &str);
string write(string key , string value);
string read (string key);
string count();
string remove(string key);

node_t *head = NULL;
node_t *tail = NULL;

int main(int argc, char ** argv) {
  int portno; 
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  portno = atoi(argv[1]);

  SA_IN server_addr , client_addr ;
  memset(&server_addr,0,sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  server_addr.sin_port = htons(portno);

  for(int i = 0 ; i < THREADPOOL ; i++)
  {
      pthread_create(&thread_pool[i],NULL,thread_function,NULL);
  }

  int server_sock = check(socket(AF_INET, SOCK_STREAM, 0),"Socket faild to exist");
  int reuse = 1;
  int reuseSock = check(setsockopt(server_sock,SOL_SOCKET,SO_REUSEADDR,&reuse,sizeof(reuse)),"Setsockopt failed");
  int bindStatus = check(bind(server_sock, (SA *) &server_addr, sizeof(server_addr)),"Error binding socket to local address");

  #ifdef LOGGING
        cout << "Waiting for a client to connect..." << endl;
  #endif

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

      int *pclient = (int *)malloc(sizeof(int));
      *pclient = client_sock;

      pthread_mutex_lock(&mutex);
      enqueue(pclient);
      pthread_cond_signal(&cond_var);
      pthread_mutex_unlock(&mutex);
  }
  close(server_sock);
  #ifdef LOGGING
      cout << "server socket finished " << server_sock << endl;
  #endif
   return 0;

}

void *thread_function(void *arg)
{   
  while (true)
  {
    int *pclient;
    pthread_mutex_lock(&mutex);
    if((pclient = dequeue()) == nullptr)
    {
      pthread_cond_wait(&cond_var,&mutex);
      pclient = dequeue();
    }
    pthread_mutex_unlock(&mutex);
    if(pclient != nullptr)
    {
      handle_connection(pclient);
    }
  }
}

int check(int code , const char *msg)
{
  if(code == SOCKERROR){
    perror(msg);
    exit(0);
  }
  return code;
}

void enqueue(int *client_socket)
{
  node_t *newnode = (node_t *)malloc(sizeof(node_t));
  newnode->client_socket = client_socket;
  newnode->next = NULL;
  if (tail == NULL)
  {
    head = newnode;
  }
  else
  {
    tail->next = newnode;
  }
  tail = newnode;
}

int *dequeue()
{
  if (head == NULL)
  {
    return NULL;
  }
  else
  {
    int *result = head->client_socket;
    node_t *temp = head;
    head = head->next;
    if (head == NULL)
    {
        tail = NULL;
    }
    free(temp);
    return result;
  }
}

void *handle_connection(void *p_client_socket)
{
  #ifdef LOGGING
      cout << "In handle connection" <<endl;
  #endif
  int client_sock = *((int *)p_client_socket);
  free(p_client_socket);
  recv(client_sock, client_message, 1024, 0);

  istringstream iss(client_message);
  string line;
  

  while(getline(iss ,line))
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
        pthread_mutex_lock(&mutex);
        const char* done = remove(key).c_str();
        pthread_mutex_unlock(&mutex);
        write(client_sock,done,strlen(done));

    } 
    else if (line == "COUNT") 
    {
        #ifdef LOGGING
            cout << "Performing COUNT operation. Number of KV pairs: " << endl;
        #endif
        pthread_mutex_lock(&mutex);
        const char* count_value = count().c_str();
        pthread_mutex_unlock(&mutex);
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
        pthread_mutex_lock(&mutex);
        const char* value_read = read(key).c_str();
        pthread_mutex_unlock(&mutex);
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
        pthread_mutex_lock(&mutex);
        const char* value_wrote = write(key,value).c_str();
        pthread_mutex_unlock(&mutex);
        write(client_sock,value_wrote,strlen(value_wrote));


    } else if (line == "END") {
        #ifdef LOGGING
            cout << "Ending the connection" << endl;
        #endif
        const char* nl = "\n";
        write(client_sock,nl,strlen(nl));
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


