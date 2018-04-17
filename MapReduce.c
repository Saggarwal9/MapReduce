#include<stdio.h>
#include<stdlib.h>
#include "mapreduce.h"


void MR_Emit(char *key, char *value){

}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, 
	        Partitioner partition)
{
    printf("Number of arguments %d\n", argc);
    
    for(int i=1;i<argc;i++)
    {
      printf("Argument %d %s\n",i,argv[i]);
    
    }
    
       
}

