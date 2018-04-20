#include<stdio.h>
#include<stdlib.h>
#include "mapreduce.h"
#include<string.h>

/////////////////////////////////////////////////////////////////////////////////
//Global Variables
/////////////////////////////////////////////////////////////////////////////////
struct table** p; //Partitions array
Partitioner partitioner;
int TABLE_SIZE=101;
int partition_number;

//Testing function for partitions/tables/nodes/subnodes
void manualtester(){
    //struct table *t=p[0];
    //struct node *temp = t->list[1];
	//temp=temp->next;
	//temp=temp->next;
	//temp=temp->next;
	//temp=temp->next;
	//struct subnode *sub= temp->subnode;
	//while(sub){
	//printf("KEY %s,  VALUE %s\n",temp->key,sub->val);
	//sub=sub->next;
	//}    
}



////////////////////////////////////////////////////////////////////////////////
//HASH STUFF
///////////////////////////////////////////////////////////////////////////////


//Bucket Element
struct node{
    char* key;
    struct node *next;
    struct subnode *subnode;
};

//Main Hash Table
struct table{
    int size;
    struct node **list;

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
    int i;
    for(i=0;i<size;i++)
        t->list[i] = NULL;
    return t;
}


unsigned long
hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}


//Insert into the hash map.
void insert(struct table *t,char* key,char* val){
    long pos=hash(key);
    pos=pos%(t->size);
    struct node *list = t->list[pos];
    struct node *newNode = malloc(sizeof(struct node));
    struct node *temp = list;
    
    struct subnode *newSubNode=malloc(sizeof(struct subnode));
    newSubNode->val=strdup(val);
    newSubNode->next=NULL;
    while(temp){
        if(strcmp(temp->key,key)==0){
            struct subnode *sublist = list->subnode;
            newSubNode->next=sublist;
            list->subnode = newSubNode;
            return;
        }
        temp = temp->next;
    }
    newNode->key = key;
    newNode->subnode = malloc(sizeof(struct subnode));
    newNode->subnode=newSubNode;    
    newNode->next = list;
    t->list[pos] = newNode;

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
    long partitionNumber;
    int found=0;
    if(partitioner!=NULL){
        partitionNumber = partitioner(key,partition_number);
    }
    else{
        partitionNumber= MR_DefaultHashPartition(key,partition_number);
    }
    
    struct table *t=p[partitionNumber];
    
    long pos=hash(key);
    pos=pos%(t->size); 
    struct node *list = t->list[pos];
    struct node *newNode = malloc(sizeof(struct node));
    struct node *temp = list;
    struct node *prev = temp;
    //printf("-1\n");
    if(temp==NULL)
        return NULL;
    while(temp){
        if(strcmp(temp->key,key)==0){
            found=1;
            //printf("found key: %s\n", temp->key);
            if(temp->subnode==NULL)
                break;
                
            struct subnode *sublist = temp->subnode;
            
            if(sublist->next!=NULL){
                //printf("Sublist->value: %s\n",sublist->val);
                temp->subnode=sublist->next;
                char *val=strdup(sublist->val);
                free(sublist->val);
                free(sublist);
                sublist=NULL;
                //return sublist->val;
                return val;
            }
            else{
                //printf("Sublist->value: %s\n",sublist->val);
                temp->subnode=NULL;
                prev->next=temp->next;
                char *val=strdup(sublist->val);
                free(sublist->val);
                free(sublist);
                free(temp);
                return val;
            }
            
        }
        else{
            prev=temp;
            //printf("PREV %p\n",prev);
            //prev->next=temp;
            temp = temp->next;
        }
    }
    if(found==0){
        free(temp);
        return NULL;
    }
    //prev->next=temp->next;
    //free(temp);
    //free(prev);
    //prev=NULL;
    //temp=NULL;
    // return NULL;    
}


//////////////////////////////////////////////////////////////////////////
// Master
//////////////////////////////////////////////////////////////////////////

//File descriptor of the files passed
struct fd{
    int fileIndex;
    int fileSize;
};



//Default Hash partitioner
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}



//
void MR_Emit(char *key, char *value){ //Key -> tocket, Value -> 1
    //printf("INSERTING: %s, %s\n",key,value);
    long partitionNumber;
    if(partitioner!=NULL){
        partitionNumber = partitioner(key,partition_number);
    }
    else{
        partitionNumber= MR_DefaultHashPartition(key,partition_number);
    } 
    
    //printf("%p\n",p[partitionNumber]);
    //insert(p[partitionNumber],key,value);    
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
    //printf("%s\n", argv[1]);
    struct fd files[argc];
    partitioner=partition;
    partition_number=num_reducers; //Number of partitions
    //Sorting the files
    for(int i=1;i<argc;i++)
    {
        FILE *file= fopen(argv[i], "r"); //r-->read mode.
        files[i-1].fileIndex=i; //argv[i] has name, i stores the index.
        files[i-1].fileSize=fileSize(file); //file size used to sort.
        fclose(file);
    }
    qsort(files,argc-1,sizeof(struct fd), compareFile); //Files sorted in descending order of size.
    
    p=malloc(sizeof(struct table *)*num_reducers);
    for(int i=0;i<partition_number;i++){
        struct table *table = createTable(TABLE_SIZE);
        p[i]=table;
    }
    
    //t=p[0];
    for(int i=1;i<argc;i++){
        //printf("File name: %s\n", argv[files[i].fileIndex]);
        map(argv[files[i-1].fileIndex]); //TODO: use thread create.
    }
    
    reduce("hello", get_next, num_reducers);
    reduce("my", get_next, num_reducers);
    reduce("name", get_next, num_reducers);
    reduce("is", get_next,num_reducers);
    reduce("lol", get_next,num_reducers);
    
}    


