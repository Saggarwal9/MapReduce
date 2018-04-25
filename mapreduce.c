////////////////////////////////////////////////////////////////////////////////
// Includes
////////////////////////////////////////////////////////////////////////////////

#include<stdio.h>
#include<stdlib.h>
#include "mapreduce.h"
#include<string.h>
#include<pthread.h>
#include <unistd.h>

/////////////////////////////////////////////////////////////////////////////////
//Global Variables
/////////////////////////////////////////////////////////////////////////////////

struct table** p; //Partitions array
Partitioner partitioner;
Mapper mapper;
Reducer reducer;
int TABLE_SIZE=1543;
int partition_number;
pthread_mutex_t filelock=PTHREAD_MUTEX_INITIALIZER;
int fileNumber;
int current_file;
int current_partition;
pthread_mutex_t *partitionlock;
struct node** reducenode;

////////////////////////////////////////////////////////////////////////////////
//HASH STUFF
///////////////////////////////////////////////////////////////////////////////

//Bucket Element
struct node{
    char* key;
    struct node *next;
    struct subnode *subnode;
    struct subnode *current;
    int size;
};

//Main Hash Table
struct table{
    int size;
    struct node **list;
    pthread_mutex_t *locks;
    long long nodesize;
    pthread_mutex_t keylock;

};

//Key Linked List
struct subnode{
    char* val;
    struct subnode* next;
};

//Function used to instantiate the table
struct table *createTable(int size){
    struct table *t = (struct table*)malloc(sizeof(struct table));
    t->size = size;
    t->list = malloc(sizeof(struct node*)*size);
    t->locks=malloc(sizeof(pthread_mutex_t)*size);
    pthread_mutex_init(&t->keylock,NULL);
    t->nodesize=0;
    int i;
    for(i=0;i<size;i++){
        t->list[i] = NULL;
        if(pthread_mutex_init(&(t->locks[i]), NULL)!=0)
            printf("Locks init failed\n");
    }
    return t;
}

 unsigned long long
 hash(char *str)
 {
     unsigned long long hash = 5381;
     int c;
 
     while((c = *str++)!= '\0'){
         hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
     }
     hash=hash%TABLE_SIZE;
     return hash;
 }

//Insert into the hash map.
void insert(struct table *t,char* key,char* val){  
    long long pos=hash(key);
    pthread_mutex_t *lock = t->locks + pos;
    struct subnode *newSubNode=malloc(sizeof(struct subnode));
    newSubNode->val=strdup(val);
    newSubNode->next=NULL;
    pthread_mutex_lock(lock);
    struct node *list = t->list[pos];
    struct node *temp = list;
    while(temp){
        if(strcmp(temp->key,key)==0){
            struct subnode *sublist = temp->subnode;
            newSubNode->next=sublist;
            temp->size++;
            temp->subnode = newSubNode;
            temp->current = newSubNode;
            pthread_mutex_unlock(lock);
            return;
        }
        temp = temp->next;
    }
    pthread_mutex_lock(&t->keylock);
    t->nodesize++;
    pthread_mutex_unlock(&t->keylock);
    struct node *newNode = malloc(sizeof(struct node));
    newNode->key=strdup(key);
    newNode->subnode=newSubNode;
    newNode->current=newNode->subnode;    
    newNode->next = list;
    t->list[pos] = newNode;
    pthread_mutex_unlock(lock);
}

//////////////////////////////////////////////////////////////////////////
// Worker
/////////////////////////////////////////////////////////////////////////

 //Method used for qsort to sort our list of keys in the hashmap.
 int compareKey(const void *s1, const void *s2)
 {
     struct node **n1 = (struct node **)s1;
     struct node **n2 = (struct node **)s2;
     if(*n1==NULL && *n2==NULL) {
         return 0;
     } else if(*n1==NULL) {
         return -1;
     } else if(*n2==NULL) {
         return 1;
     } else {
     return strcmp((*n1)->key,(*n2)->key);
     }
 }


//Getter function that returns the next key in a subnode list.
char* get_next(char* key, int partition_num)
{
    struct node *tempnode= reducenode[partition_num];
    struct subnode *addr = tempnode->current;
    if(addr==NULL){
        return NULL;
      }
     tempnode->current=addr->next;
     return addr->val;
}

//////////////////////////////////////////////////////////////////////////
// File Stuff
//////////////////////////////////////////////////////////////////////////

void* callMap(char *fileName){
    mapper(fileName);
    return NULL;
}

void reduceHelper(int i){
    if(p[i]==NULL)
        return;
    struct table* tempTable=p[i];
    struct node *list[p[i]->nodesize];
    long long x=0;
    for(int j=0;j<TABLE_SIZE;j++){
        if(tempTable->list[j] ==NULL)
            continue;
        struct node* tempNode=tempTable->list[j];
        while(tempNode){
            list[x]=tempNode;
            x++;
            tempNode=tempNode->next;
        }
    }
    qsort(list,p[i]->nodesize,sizeof(struct node *),compareKey);
    for(int k=0;k<x;k++){
        reducenode[i]=list[k];
        reducer(list[k]->key,get_next,i);
    }
}
        
         
void* callReduce(){
    while(1){
        pthread_mutex_lock(&filelock);
        int x;
        if(current_partition>=partition_number){
            pthread_mutex_unlock(&filelock);
            return NULL;
        }
        if(current_partition<partition_number){
            x=current_partition;
            current_partition++;
        }
        pthread_mutex_unlock(&filelock);
	    reduceHelper(x);
    }    
}

void *findFile(void *files)
{    
    char **arguments = (char**) files;
    while(1){
        pthread_mutex_lock(&filelock);
        int x;
        if(current_file>fileNumber){
            pthread_mutex_unlock(&filelock); 
            return NULL;
        }
        if(current_file<=fileNumber){
            x=current_file;
            current_file++;
        }
        pthread_mutex_unlock(&filelock); 
        callMap(arguments[x]);  
    }
    return NULL;
}

//////////////////////////////////////////////////////////////////////////
// Master
//////////////////////////////////////////////////////////////////////////

//Default Hash partitioner
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
    //return 1;
}

//
void MR_Emit(char *key, char *value){ //Key -> tocket, Value -> 1
    long partitionNumber;
    //printf("In MR_EMIT\n");
    if(partitioner!=NULL){
        partitionNumber = partitioner(key,partition_number);
    }
    else{
        partitionNumber= MR_DefaultHashPartition(key,partition_number);
    } 
    insert(p[partitionNumber],key,value);    
}

////////////////////////////////////////////////////////////////////////////
// Memory Cleanup
////////////////////////////////////////////////////////////////////////////

void freeTable(struct table *t)
{
    for(int i=0;i<t->size;i++)
    {
	    struct node *list1=t->list[i];
        struct node *temp2=list1;
        pthread_mutex_t *l=&(t->locks[i]);
        while(temp2)
        { 	
            struct node *tempNode=temp2;
            struct subnode *temp=tempNode->subnode;
            while(temp){
                struct subnode *sublist=temp;
                temp=temp->next; 
		        free(sublist->val);	
                free(sublist);
            }
            temp2=temp2->next;
		    free(tempNode->key);
            free(tempNode); 
	    }
	    pthread_mutex_destroy(l);
    }
    free(t->locks);
    free(t->list);
    free(t);
}

////////////////////////////////////////////////////////////////////////////
/* Main */
///////////////////////////////////////////////////////////////////////////

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, 
	        Partitioner partition)
{   
    //printf("IN MR_RUN\n");
    current_partition=0;
    current_file=1;
    partitioner=partition;
    mapper=map;
    reducer=reduce;
    partition_number=num_reducers; //Number of partitions
    fileNumber=argc-1;
    p=malloc(sizeof(struct table *)*num_reducers);
    
    //Create Partitions
    for(int i=0;i<partition_number;i++){
        struct table *table = createTable(TABLE_SIZE);
        p[i]=table;
    }
    
    //Start Mapping Process
    pthread_t mappers[num_mappers];
    for(int i=0;i<num_mappers || i==argc-1;i++){
         pthread_create(&mappers[i], NULL,findFile,(void*) argv);
    } 
    //Join the Mappers       
    for(int i=0;i<num_mappers || i==argc-1;i++)//Start joining all the threads
    {
            pthread_join(mappers[i], NULL);
    }

    //Start Reducing Process
    pthread_t reducer[num_reducers];
    reducenode=malloc(sizeof(struct node *) * num_reducers);
    for(int i=0;i<num_reducers;i++){
        pthread_create(&reducer[i],NULL,callReduce,NULL);
    }
    
    //Join the Reducers
    for(int i=0;i<num_reducers;i++){
        pthread_join(reducer[i],NULL);
    }
    
    //Memory clean-up
    for(int i=0;i<partition_number;i++)
	{	
  	 	freeTable(p[i]);
	}
    free(partitionlock);
    free(reducenode);
    free(p);
}    
    
