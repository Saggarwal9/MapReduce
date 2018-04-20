#include<stdio.h>
#include<stdlib.h>
#include "mapreduce.h"
#include<string.h>

////////////////////////////////////////////////////////////////////////////////
//HASH STUFF
///////////////////////////////////////////////////////////////////////////////

int num_partitions=512;
Partitioner partitioner;


struct partition{
    struct table* hashTable;
};

//Bucket Element
struct node{
    char* key;
    char* val;
    struct node *next;
};

//Main Hash Table
struct table{
    int size;
    struct node **list;
    int wordCount;
    int bucketCount;
};


struct table *t; //Hash Table Variable

//Function used to instantiate the table
struct table *createTable(int size){
    struct table *t = (struct table*)malloc(sizeof(struct table));
    t->size = size;
    t->list = (struct node**)malloc(sizeof(struct node*)*size);
    int i;
    for(i=0;i<size;i++)
        t->list[i] = NULL;
    return t;
}

//Default Hash partitioner
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}


//Insert into the hash map.
void insert(struct table *t,char* key,char* val){
    long pos;
    if(partitioner!=NULL){
        pos = partitioner(key,num_partitions);
    }
    else{
        pos= MR_DefaultHashPartition(key,num_partitions);
      }
      
    struct node *list = t->list[pos];
    struct node *newNode = (struct node*)malloc(sizeof(struct node));
    struct node *temp = list;
    newNode->key = strdup(key);
    newNode->val = strdup(val);
    printf("Inserting Key: %s Val: %s at index: %li\n",newNode->key,newNode->val,pos); 
    newNode->next = NULL;
    if(temp==NULL){ //Bucket doesn't exist
        t->list[pos] = newNode;
        t->bucketCount++;
    }
    else{ //Add in front of the bucket-list
        newNode->next=temp;
        t->list[pos]=newNode;
    }
}


//THIS IS REDUCE WORK --> used for debug.
//long lookup(struct table *t,int key,Partitioner partition){
    //if(partition!=NULL)
        //long pos = partition(key,num_partitions);
    //else
        //long pos= MR_DefaultHashPartition(key,num_partitions);
    //struct node *list = t->list[pos];
    //struct node *temp = list;
    //while(temp){
        //if(temp->key==key){
            //return temp->val;
       // }
        //temp = temp->next;
   // }
    //return -1;
//}







//////////////////////////////////////////////////////////////////////////
// Worker
/////////////////////////////////////////////////////////////////////////


//Method used for qsort to sort our list of keys in the hashmap.
int compareKey(const void *s1, const void *s2)
{
    struct node **n1 = (struct node **)s1;
    struct node **n2 = (struct node **)s2;
    //printf("s1: %p, s2: %p\n", s1, s2);
    if(*n1==NULL && *n2==NULL) {
      //  printf("do have NULL\n");
        return 0;
    } else if(*n1==NULL) {
       // printf("do have NULL\n");
        return -1;
    } else if(*n2==NULL) {
      //  printf("do have NULL\n");
        return 1;
    } else {
    printf("No NULL: \n");
    printf("*n1: %p, *n2: %p\n", *n1, *n2);
    return strcmp((*n1)->key,(*n2)->key);
    }
}






//?
char* get_next(char* key, int partition_number)
{
    
}


//////////////////////////////////////////////////////////////////////////
// Master
//////////////////////////////////////////////////////////////////////////


//File descriptor of the files passed
struct fd{
    int fileIndex;
    int fileSize;
    int inUse=0;
};


//
void MR_Emit(char *key, char *value){ //Key -> tocket, Value -> 1
    printf("INSERTING: %s, %s\n",key,value); 
    insert(t,key,value);    
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
    printf("%s\n", argv[1]);
    struct fd files[argc];
    partitioner=partition;
    for(int i=1;i<argc;i++)
    {
        FILE *file= fopen(argv[i], "r"); //r-->read mode.
        files[i-1].fileIndex=i; //argv[i] has name, i stores the index.
        files[i-1].fileSize=fileSize(file); //file size used to sort.
        fclose(file);
    }
    qsort(files,argc-1,sizeof(struct fd), compareFile); //Files sorted in descending order of size.
    t = createTable(512);
    for(int i=1;i<argc;i++){
        //printf("File name: %s\n", argv[files[i].fileIndex]);
        map(argv[files[i-1].fileIndex]); //TODO: use thread create.
    }
}

