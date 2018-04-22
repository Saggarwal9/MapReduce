#include<stdio.h>
#include<stdlib.h>
#include "mapreduce.h"
#include<string.h>
#include<pthread.h>

/////////////////////////////////////////////////////////////////////////////////
//Global Variables
/////////////////////////////////////////////////////////////////////////////////
struct table** p; //Partitions array
Partitioner partitioner;
Mapper mapper;
Reducer reducer;
int TABLE_SIZE=501;
int partition_number;
pthread_mutex_t filelock=PTHREAD_MUTEX_INITIALIZER;
int MAPDONE=0;
int fileNumber=0;
int current_file=1;
int map_thread=0;
pthread_mutex_t *partitionlock;

////////////////////////////////////////////////////////////////////////////////
//HASH STUFF
///////////////////////////////////////////////////////////////////////////////



//Bucket Element
struct node{
    char* key;
    struct node *next;
    struct subnode *subnode;
    struct subnode *current;
    //get next helpers
    //int current;
    //int size;
};

//Main Hash Table
struct table{
    int size;
    struct node **list;
    pthread_mutex_t *locks;

};

//Key Linked List
struct subnode{
    char* val;
    struct subnode* next;
};

//struct table *t; //Hash Table Variable

//Function used to instantiate the table
struct table *createTable(int size){
    struct table *t = (struct table*)malloc(sizeof(struct table));
    t->size = size;
    t->list = malloc(sizeof(struct node*)*size);
    t->locks=malloc(sizeof(pthread_mutex_t)*size);
    if(t->locks == NULL){
        free(t);
        return NULL;
    }
    int i;
    for(i=0;i<size;i++){
        t->list[i] = NULL;
        pthread_mutex_init(&(t->locks[i]), NULL);
    }
    return t;
}


unsigned long long
hash(char *str)
{
    unsigned long long hash = 5381;
    int c;

    while ((c = *str++)){
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    hash=hash%TABLE_SIZE;
    return hash;
    //return 1;
}


//Insert into the hash map.
void insert(struct table *t,char* key,char* val){
    if(strcmp(key,"")==0)
       return;
    long long pos=hash(key);
    struct node *list = t->list[pos];
    struct node *temp = list;
    pthread_mutex_t *lock = t->locks + pos;
    struct subnode *newSubNode=malloc(sizeof(struct subnode));
    newSubNode->val=strdup(val);
    newSubNode->next=NULL;
    pthread_mutex_lock(lock);
    int i=0;
    while(temp){
        if(strcmp(temp->key,key)==0){
            printf("i: %d, key: %s\n",i,key);
            struct subnode *sublist = temp->subnode;
            newSubNode->next=sublist;
            //temp->size++;
            temp->subnode = newSubNode;
            temp->current=temp->subnode;
            pthread_mutex_unlock(lock);
            return;
        }
        
        temp = temp->next;
        i++;
    }
    //printf("Key %s\n",key);
    struct node *newNode = malloc(sizeof(struct node));
    temp=list;
    
    struct node* prev=temp;
    
    newNode->key = strdup(key);
    newNode->subnode=newSubNode;
    //newNode->size=1;
    newNode->current=newNode->subnode;
    if(temp==NULL){
        t->list[pos]= newNode;
        newNode->next=NULL;
        pthread_mutex_unlock(lock);
        return;
    }
    else{
        int count=0;
        while(temp){
           if(strcmp(temp->key,key)>0){
                break;
            }
            count++;
            prev=temp;
            temp=temp->next;
        }
        
        if(count==0){
            newNode->next=temp;
            t->list[pos]=newNode;
            pthread_mutex_unlock(lock);
            return;
        }
        newNode->next=temp;
        prev->next=newNode;
        pthread_mutex_unlock(lock);
        return;
    }
    
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
char* get_next(char* key, int partition_number)
{
    if(strcmp(key,"")==0)
       return NULL;
    long partitionNumber;
    int found=0;
    if(partitioner!=NULL){
        partitionNumber = partitioner(key,partition_number);
    }
    else{
        partitionNumber= MR_DefaultHashPartition(key,partition_number);
    }
    
    pthread_mutex_lock(&partitionlock[partition_number]);
    struct table *t=p[partitionNumber];
    
    long long pos=hash(key);
    struct node *list = t->list[pos];
    struct node *temp = list;
    while(temp){
        if(strcmp(temp->key,key)==0){
            //printf("Here\n");
            found=1;
            break;
         }
         else{
            temp=temp->next;
         }       
    }
    if(found==0){ //key not found
        pthread_mutex_unlock(&partitionlock[partition_number]);
        return NULL;
     }
    
    //struct subnode *sublist=temp->subnode;
    struct subnode *addr = temp->current;
    if(addr==NULL){
         pthread_mutex_unlock(&partitionlock[partition_number]);
        return NULL;
      }
    //printf("Key: %s\n",key);
     //char* str=strdup(addr->val);
     temp->current=addr->next;
      pthread_mutex_unlock(&partitionlock[partition_number]);
     return addr->val;
}

//////////////////////////////////////////////////////////////////////////
// File Stuff
//////////////////////////////////////////////////////////////////////////

//File descriptor of the files passed
struct fd{
    int fileIndex;
    int fileSize;
};

void* callMap(char *fileName){
    mapper(fileName);
    return NULL;
}

void* callReduce(void *key){
    printf("In callreduce\n");
    long long partitionNumber;
    if(partitioner!=NULL){
        partitionNumber = partitioner(key,partition_number);
    }
    else{
        partitionNumber= MR_DefaultHashPartition(key,partition_number);
    }
    partitionNumber=10;
    char *string=(char*)key;
    reducer(string,get_next,partitionNumber);
    return NULL;
}

int fileCheck(){   
    map_thread++;
    int file1=-1;
    if(current_file<=fileNumber)
    {
        file1=current_file;
        current_file++;
        pthread_mutex_unlock(&filelock);   
        return file1;
    }
    MAPDONE=1;
    pthread_mutex_unlock(&filelock); 
    return file1;
}

void *findFile(void *files)
{    
    //int i=0;
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


//Used for qsort().
int compareFile(const void *s1, const void *s2)
{
    struct fd *f1 = (struct fd *)s1;
    struct fd *f2 = (struct fd *)s2;
    return (f2->fileSize-f1->fileSize);
        
}


//Compute file size using pointers.
int fileSize(FILE *fp)
{
        int prev=ftell(fp); //Current Offset
        fseek(fp,0L,SEEK_END); //End of File
        int size=ftell(fp); //EOF Offset
        fseek(fp,prev,SEEK_SET); //Back to Prev Offset
        return size;
}


////////////////////////////////////////////////////////////////////////////
/* Main */
///////////////////////////////////////////////////////////////////////////


//main program
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, 
	        Partitioner partition)
{   
    partitioner=partition;
    mapper=map;
    reducer=reduce;
    partition_number=num_reducers; //Number of partitions
    fileNumber=argc-1;

    //Sorting the files
    //for(int i=0;i<argc;i++)
    //{
       // FILE *file= fopen(argv[i], "r"); //r-->read mode.
        //files[i].fileIndex=i+1; //argv[i] has name, i stores the index.
        //files[i].fileSize=fileSize(file); //file size used to sort.
        //fclose(file);
    //}
    //qsort(files,argc-1,sizeof(struct fd), compareFile); //Files sorted in descending order of size.
     
    p=malloc(sizeof(struct table *)*num_reducers);
    
    for(int i=0;i<partition_number;i++){
        struct table *table = createTable(TABLE_SIZE);
        p[i]=table;
    }
    
    pthread_t mappers[num_mappers];
       
        for(int i=0;i<num_mappers;i++){
            pthread_create(&mappers[i], NULL,findFile,(void*) argv);
        } 
            
    for(int i=0;i<num_mappers;i++)//Start joining all the threads
        {
            //printf("Joining\n");
            if(i==argc-1)
                break;
            pthread_join(mappers[i], NULL);
        }
    
    pthread_mutex_destroy(&filelock); //Don't need the file lock anymore
    
    printf("Insert done\n");
    
    
    partitionlock=malloc(sizeof(pthread_mutex_t)*num_reducers);    
    for(int i=0;i<num_reducers;i++){
        pthread_mutex_init(&partitionlock[i],NULL);    
    }
    

    //pthread_t reducer[num_reducers];
    //char* string=strdup("yourself");
    //pthread_create(&reducer,NULL,callReduce,(void*)string);
    
    //pthread_join(reducer,NULL);
    
    //TEST
    
    FILE *fp = fopen(argv[1], "r");
    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
           reduce(token,get_next, num_reducers);
        }
    }

    fclose(fp); 
       
}    

