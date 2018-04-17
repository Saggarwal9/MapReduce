#include<stdio.h>
#include<stdlib.h>
#include "mapreduce.h"

struct fd{
    int fileIndex;
    int fileSize;
};


int compare(const void *s1, const void *s2)
{
    struct fd *f1 = (struct fd *)s1;
    struct fd *f2 = (struct fd *)s2;
    //int sizeCompare = strcmp(e1->gender, e2->gender);
   /* if(&s1.fileSize>&s2.fileSize)
    {
       return s1.fileSize;      
    }
    if (gendercompare == 0)  // same gender so sort by id //
        return e1->id - e2->id;
    else
        return -gendercompare;*/
        return (f1->fileSize-f2->fileSize);
        
}


int fileSize(FILE *fp)
{
        int prev=ftell(fp); //Current Offset
        fseek(fp,0L,SEEK_END); //End of File
        int size=ftell(fp); //EOF Offset
        fseek(fp,prev,SEEK_SET); //Back to Prev Offset
        return size;
}

void MR_Emit(char *key, char *value){ //Key -> tocket, Value -> 1

}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, 
	        Partitioner partition)
{
    printf("Number of arguments %d\n", argc);
    
    struct fd files[argc];
    for(int i=1;i<argc;i++)
    {
        FILE *file= fopen(argv[i], "r"); //r-->read mode.
        files[i-1].fileIndex=i; //argv[i] has name, i stores the index.
        files[i-1].fileSize=fileSize(file); //file size used to sort.
        fclose(file);
    }
    qsort(files,argc-1,sizeof(struct fd), compare);
    printf("%s\n", argv[1]);
    map(argv[1]);
      
}


