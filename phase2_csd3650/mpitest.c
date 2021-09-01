#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <stddef.h>
#include <string.h>
#include <time.h>
#include <math.h>
#define TAG 0
#define SERVER 1
#define CLIENT 2
#define ACK 3
#define START_LEADER_ELECTION 4
#define LEADER_ELECTION_DONE 5
#define CANDIDATE_ID 6
#define CONNECT 7
#define UPLOAD 8
#define CLIENT_TO_LEADER 9
#define UPLOAD_FAILED 10
#define LONG_ACNE 11
#define UPLOAD_ACK 12
#define UPLOAD_OK 13
#define RETRIEVE 14
#define RETRIEVE_FAILED 15
#define RETRIEVE_OK 16
#define RETRIEVE_ACK 17
#define VERSION_CHECK 19
#define VERSION_OUTDATED 20


struct catalogue {
    int data;
    int version;
    struct catalogue *next;
};
struct catalogue *chead;
struct registration{
    int key;
    struct queue *front;
    struct queue *back;
};
struct registration  hash_table[1000];
struct queue {
    int client_id;
    int count;
    int type;
    int version;
    struct queue *next;
} ;

int main(int argc, char *argv[]){

    int rank, NUM_SERVERS,NUM_PROC, i;
    /** MPI Initialisation **/
    MPI_Init(&argc, &argv);
    NUM_PROC = atoi(argv[2]);

    MPI_Comm_size(MPI_COMM_WORLD, &NUM_SERVERS);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Status status;
    int *buffer,neighbors[2];
    unsigned  int tag;
    int dummy_int=2;
    int nothing=1;
    int processes[NUM_SERVERS];
    for(i=0;i<NUM_SERVERS;i++)processes[i]=i;
    //printf("Servers are : %d \n",atoi(argv[1]));
    //printf("File name : %s \n",argv[2]);
    int shortest_path(int arr[],  int x, int y){
        int i, j;
        int n = atoi(argv[1]);
        int min_dist ;
        for (i = 0; i < n; i++)if(arr[i]==x)break;
        for (j = 0; j < n; j++)if(arr[j]==y)break;

        return (i-j);
    }
    int push(struct catalogue** head_ref, int new_data,int new_version){
        struct catalogue* new_node = (struct catalogue*) malloc(sizeof(struct catalogue));
        new_node->data  = new_data;
        new_node ->version = new_version;
        new_node->next = (*head_ref);
        (*head_ref) = new_node;
        return new_data;
    }
    void delete_request(int index, int type){
        printf("deleted index %d , type %d\n",index,type);
        struct queue *temp = hash_table[index].front , *prev;
        while (temp != NULL && temp->type != type){
            prev = temp;
            temp = temp->next;
        }
        if (temp == NULL) return;
        prev->next = temp->next;
    }
    void cat_insert(struct catalogue* head_ref,int id,int version){
        struct catalogue* new_node = head_ref;
        new_node->data  = id;
        new_node->version = version;
        new_node->next = head_ref;
        head_ref = new_node;
    }
    void enqueue(int index,int type,int cid,int version){
        struct queue * n = malloc(sizeof(struct queue));
        n->client_id = cid;
        n->version = version;
        n->count = (atoi(argv[1]) / 2 + 1);
        n->type = type;
        n->next = NULL;
        hash_table[index].back->next = n;
        hash_table[index].back = n;
    }
    void dequeue(int index){
        if( hash_table[index].front==NULL){
            printf("wtf\n");
            return;
        }
        //struct queue * n= hash_table[index].front;
        hash_table[index].front = hash_table[index].front->next;
        if (hash_table[index].front == NULL)hash_table[index].back = NULL;
    }
    int cat_search(struct catalogue * head_ref,int x){
        while (head_ref != NULL){
            if (head_ref->data == x)
                return head_ref->version;
            head_ref = head_ref->next;
        }
        return 0;
    }
    if(rank == 0){
        // CoordinatorMPI_INT;
        printf("[rank: %d] Coordinator started\n", rank);
        char const* const fileName = argv[2];
        FILE* file = fopen(fileName, "r");
        char *line;
        ssize_t len =0;
        char * split;
        //printf("numservers %d\n",NUM_SERVERS);
        int arg[10];
        int destination;
        while (getline(&line, &len, file)!=-1) {
            i = 0;
            split = strtok(line, " ");
            if (!strcmp(split, "SERVER")) {
                while (split != NULL) {

                    arg[i++] = atoi(split);
                    split = strtok(NULL, " ");
                }
                neighbors[0] = arg[2];
                neighbors[1] = arg[3];
                processes[arg[1]] = SERVER;
                // printf("Server sent : %d %d %d \n", arg[1], neighbors[0], neighbors[1]);
                // printf("source %d,destination %d\n", rank, arg[1]);
                MPI_Send(&neighbors, 2, MPI_INT, arg[1], SERVER, MPI_COMM_WORLD);
                MPI_Recv(&nothing, 1, MPI_INT, arg[1], ACK, MPI_COMM_WORLD, &status);
            }else if(strstr(line,"START_LEADER_ELECTION")!=NULL){
                for(i=0;i<NUM_SERVERS;i++){
                    if(processes[i] == SERVER){
                        MPI_Send(&nothing, 1, MPI_INT, i, START_LEADER_ELECTION, MPI_COMM_WORLD);
                    }
                }
                MPI_Recv(&nothing, 1, MPI_INT, MPI_ANY_SOURCE, LEADER_ELECTION_DONE, MPI_COMM_WORLD, &status);
                int leader = status.MPI_SOURCE;
                for(i=1;i<NUM_SERVERS;i++){
                    if(processes[i]!=SERVER) {
                        processes[i] = CLIENT;
                        MPI_Send(&leader, 1, MPI_INT, i, CLIENT, MPI_COMM_WORLD);
                    }
                }
                MPI_Recv(0, 0, MPI_INT, MPI_ANY_SOURCE, ACK, MPI_COMM_WORLD, &status);
            }else if(strstr(line,"UPLOAD")!=NULL){
                while (split != NULL) {
                    arg[i++] = atoi(split);
                    split = strtok(NULL, " ");
                }
                int client_rank = arg[1];
                int file_id = arg[2];
                MPI_Send(&file_id, 1, MPI_INT, client_rank, UPLOAD, MPI_COMM_WORLD);
            }else if(strstr(line,"RETRIEVE")!=NULL){
                while (split != NULL) {
                    arg[i++] = atoi(split);
                    split = strtok(NULL, " ");
                }
                int client_rank = arg[1];
                int file_id = arg[2];
                printf("%d %d \n",client_rank,file_id);
                MPI_Send(&file_id, 1, MPI_INT, client_rank, RETRIEVE, MPI_COMM_WORLD);
            }

        }
        fclose(file);
    }else {
        // Peers
        int y[2];
        int client_leader;
        MPI_Recv(&neighbors, 2, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (status.MPI_TAG == SERVER) {
            chead = malloc(sizeof(struct catalogue));
            int leader_id = rank;
            int counter=0;
            int end=0 ;
            int found =0;
            int type = SERVER;
            int servers[atoi(argv[1])];
            for (i = 0; i < atoi(argv[1])-1; i++) {
                servers[i] = 0;
            }
            int l_neighbor, r_neighbor;
            int candidate_flag = 1;
            int leader_arr[atoi(argv[1])];
            int k = atoi(argv[1])-3;
            int long_acnes[k/4];
            l_neighbor = neighbors[0];
            r_neighbor = neighbors[1];
            MPI_Send(&nothing, 1, MPI_INT, 0, ACK, MPI_COMM_WORLD);
//leader election
            MPI_Recv(&y, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

            candidate_flag =1;
            if (status.MPI_TAG == START_LEADER_ELECTION && candidate_flag == 1 ) {
                y[0] = rank;
                candidate_flag = 0;
                MPI_Send(&y, 1, MPI_INT, l_neighbor, CANDIDATE_ID, MPI_COMM_WORLD);
                MPI_Recv(&y,1,MPI_INT, r_neighbor, CANDIDATE_ID, MPI_COMM_WORLD, &status);
                found =0;
                while (1) {
                    found =0;
                    if (y[0] > leader_id)leader_id = y[0];
                    for (i = 0; i < atoi(argv[1]); i++) {
                        if (servers[i] == y[0]) {
                            found = 1;
                            break;
                        }
                    }
                    if(found == 0 ) {
                        for (i = 0; i < atoi(argv[1]); i++) {
                            if (servers[i] == 0) {
                                counter++;
                                servers[i] = y[0];
                                break;
                            }
                        }
                    }

                    // printf("magkas counter %d %d\n",counter,atoi(argv[1])-1);
                    if(counter == atoi(argv[1])-1){
                        break;
                    }
                    MPI_Send(&y, 1, MPI_INT, l_neighbor, CANDIDATE_ID, MPI_COMM_WORLD);
                    MPI_Recv(&y, 1, MPI_INT, r_neighbor, CANDIDATE_ID, MPI_COMM_WORLD, &status);
                }
            }else if (status.MPI_TAG == CANDIDATE_ID && status.MPI_SOURCE == r_neighbor) {
                while (1){
                    found =0;
                    if (y[0] > leader_id){
                        leader_id = y[0];
                    }
                    if (candidate_flag == 1) {
                        candidate_flag = 0;
                        MPI_Send(&rank, 1, MPI_INT, l_neighbor, CANDIDATE_ID, MPI_COMM_WORLD);

                    }
                    for (i = 0; i < atoi(argv[1]); i++) {
                        if (servers[i] == y[0]) {
                            found = 1;
                            break;
                        }
                    }
                    if(found == 0 ) {
                        for (i = 0; i < atoi(argv[1]); i++) {
                            if (servers[i] == 0) {
                                counter++;
                                servers[i] = y[0];
                                break;
                            }
                        }
                    }
                    // printf("candi counter %d %d\n",counter,atoi(argv[1])-1);
                    if(counter == atoi(argv[1])-1){
                        break;
                    }
                    // printf("%d %d %d %d \n",counter,y,rank,leader_id);
                    MPI_Send(&y, 1 ,MPI_INT,l_neighbor,CANDIDATE_ID , MPI_COMM_WORLD);
                    MPI_Recv(&y,1,MPI_INT,r_neighbor, CANDIDATE_ID, MPI_COMM_WORLD, &status);
                }
            }
            MPI_Recv(&y, 2, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if(rank == leader_id) {
                int file[2];
                int version;
                int sent;
                int num, z, min[2];
                min[0]=999;
                int cc=0;
                min[1]=-1;
                int up[2];
                int place;
                int index;
                int acnes_tmp, neighbor_tmp;
                int upload_send[(atoi(argv[1]) / 2 + 1)];
                int l;
                servers[9] = 13;
                printf("\n");
             //   for (i = 0; i < atoi(argv[1]); i++)printf(" %d ", servers[i]);
                printf("\n");
                srand(time(NULL));
                int random = 0;
                for (i = 0; i < (atoi(argv[1]) - 3) / 4; i++) {
                    while (1) {
                        random = servers[rand() % atoi(argv[1])];
                        if (servers[random] != rank && random != r_neighbor && random != l_neighbor && random != 0) {
                            break;
                        }
                    }
                    //printf("random %d\n",servers[random]);
                    long_acnes[i] = servers[random];
                    MPI_Send(&y, 1, MPI_INT, servers[random], CONNECT, MPI_COMM_WORLD);
                    MPI_Recv(&y, 1, MPI_INT, servers[random], ACK, MPI_COMM_WORLD, &status);
                }
                MPI_Send(&y, 1, MPI_INT, 0, LEADER_ELECTION_DONE, MPI_COMM_WORLD);
                int fedon[3];
                for (z = 0; z < (atoi(argv[1]) / 2 + 1); z++) {
                    num = (rand() % (atoi(argv[1]) - 1 - 1 + 1)) + 1;
                    upload_send[z] = num;
                    for(l=0;l<z;l++){
                        while(upload_send[z]==upload_send[l]){
                            num = (rand() % (atoi(argv[1]) - 1 - 1 + 1)) + 1;
                            upload_send[z]=num;
                        }
                    }
                }
                //printf("\n");
                //for (z = 0; z < (atoi(argv[1]) / 2 + 1); z++)printf("%d ",upload_send[z]);
                //printf("\n");
                while(1) {
                    MPI_Recv(&fedon, 3, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                    if(status.MPI_TAG == CLIENT_TO_LEADER && hash_table[fedon[0]].front != NULL){
                       // MPI_Send(&fedon, 2, MPI_INT,status.MPI_SOURCE, UPLOAD_FAILED, MPI_COMM_WORLD);
                       printf("CLIENT %d FAILED TO UPLOAD %d\n",status.MPI_SOURCE,fedon[0]);
                        continue;

                    }else if(status.MPI_TAG == RETRIEVE && hash_table[fedon[0]].front == NULL ){
                        //MPI_Send(&fedon, 2, MPI_INT,status.MPI_SOURCE, UPLOAD_FAILED, MPI_COMM_WORLD);
                        printf("CLIENT FAILED TO RETRIEVE FILE %d\n",fedon[0]);
                        continue;
                    }else if(status.MPI_TAG == CLIENT_TO_LEADER && hash_table[fedon[0]].front == NULL) {
                        hash_table[fedon[0]].front = malloc(sizeof(struct registration));
                        hash_table[fedon[0]].back = malloc(sizeof(struct registration));
                        hash_table[fedon[0]].key = fedon[0];
                        struct queue * n = malloc(sizeof(struct registration));
                        n->client_id = status.MPI_SOURCE;
                        n->count = (atoi(argv[1]) / 2 + 1);
                        n->type = UPLOAD;
                        n->version =1;
                        hash_table[fedon[0]].front = n;
                        hash_table[fedon[0]].back = n;
                        //n->next = hash_table[fedon[0]].back;
                        hash_table[fedon[0]].front->count = (atoi(argv[1]) / 2 + 1);
                        for (z = 0; z < (atoi(argv[1]) / 2 + 1); z++) { //(atoi(argv[1]) / 2 + 1)
                            sent = 0;
                            acnes_tmp = 0;
                            neighbor_tmp = 0;
                            min[0] = 999;
                            min[1] = 0;
                            for (i = 0; i < k / 4; i++) {
                                acnes_tmp = shortest_path(servers, long_acnes[i], upload_send[z]);
                                if (acnes_tmp <= min[0]) {
                                    min[0] = acnes_tmp;
                                    min[1] = i;
                                }
                            }
                            neighbor_tmp = shortest_path(servers, l_neighbor, upload_send[z]);
                            if (min[0] > 0 || min[0] < acnes_tmp) {
                                up[0] = upload_send[z];
                                up[1] = fedon[0];
                                MPI_Send(&up, 2, MPI_INT, long_acnes[min[1]], UPLOAD, MPI_COMM_WORLD);
                                //printf("id %d send to %d\n",long_acnes[min[1]],l_neighbor);
                            } else {
                                up[0] = upload_send[z];
                                up[1] = fedon[0];
                                MPI_Send(&up, 2, MPI_INT, l_neighbor, UPLOAD, MPI_COMM_WORLD);
                                //printf("id %d send to %d\n",up[0],l_neighbor);
                            }
                        }
                    }else if(status.MPI_TAG == UPLOAD_ACK && rank == fedon[0]){
                        hash_table[fedon[1]].front->count --;
                        if(hash_table[fedon[1]].front->count == 0){
                            printf("CLIENT UPLOADED FILE %d \n",fedon[1]);
                            dequeue(fedon[1]);
                        }
                    }else if(status.MPI_TAG == RETRIEVE && hash_table[fedon[1]].front != NULL ){
                        enqueue(fedon[0],RETRIEVE,status.MPI_SOURCE,1);
                        int file[2];
                        int version;
                        int sent;
                        int num, z, min[2];
                        min[0]=999;
                        int cc=0;
                        min[1]=-1;
                        int up[2];
                        int place;
                        int index;
                        int acnes_tmp, neighbor_tmp;
                        //if(hash_table[fedon[0]].front->type ==RETRIEVE){
                        for (z = 0; z < (atoi(argv[1]) / 2 + 1); z++) {
                            sent = 0;
                            acnes_tmp = 0;
                            neighbor_tmp = 0;
                            min[0] = 999;
                            min[1] = 0;
                            for (i = 0; i < k / 4; i++) {
                                acnes_tmp = shortest_path(servers, long_acnes[i], upload_send[z]);
                                if (acnes_tmp <= min[0]) {
                                    min[0] = acnes_tmp;
                                    min[1] = i;
                                }
                            }
                            neighbor_tmp = shortest_path(servers, l_neighbor, upload_send[z]);
                            if (min[0] > 0 || min[0] < acnes_tmp) {
                                up[0] = upload_send[z];
                                up[1] = fedon[0];
                                MPI_Send(&up, 2, MPI_INT, long_acnes[min[1]], RETRIEVE, MPI_COMM_WORLD);
                            } else {
                                up[0] = upload_send[z];
                                up[1] = fedon[0];
                                MPI_Send(&up, 2, MPI_INT, l_neighbor, RETRIEVE, MPI_COMM_WORLD);
                            }
                        }
                        // }
                    }else if(status.MPI_TAG == RETRIEVE_ACK ){
                        hash_table[fedon[1]].front->count --;
                        if(fedon[2]>hash_table[fedon[1]].front->version)hash_table[fedon[1]].front->version = fedon[2];
                        if(hash_table[fedon[1]].front->count == 0) {
                            printf("CLIENT %d RETRIEVED FILE  %d\n", fedon[1], fedon[2]);

                        }
                    }


                }
            }else{
                if (status.MPI_TAG == CONNECT && status.MPI_SOURCE == leader_id) {
                    MPI_Send(&y, 1, MPI_INT, leader_id, ACK, MPI_COMM_WORLD);
                    type = LONG_ACNE;
                    printf("%d CONNECTED TO %d\n", rank, status.MPI_SOURCE);
                }
                int fedon[3];
                while(1) {
                    MPI_Recv(&fedon, 3, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                    if (status.MPI_TAG == UPLOAD && rank!=fedon[0]) {
                        MPI_Send(&fedon, 2, MPI_INT, l_neighbor, UPLOAD, MPI_COMM_WORLD);
                    }else if(status.MPI_TAG == UPLOAD && rank==fedon[0]){
                        //save to local
                        struct catalogue * head_ref = chead;
                        cat_insert(head_ref,fedon[1],1);
                        fedon[0] = leader_id;
                        MPI_Send(&fedon, 2, MPI_INT, l_neighbor, UPLOAD_ACK, MPI_COMM_WORLD);
                    }else if(status.MPI_TAG == UPLOAD_ACK && rank!=fedon[0]){
                        MPI_Send(&fedon, 2, MPI_INT, l_neighbor, UPLOAD_ACK, MPI_COMM_WORLD);
                    }else if(status.MPI_TAG == UPLOAD_ACK && rank!=fedon[0] && type == LONG_ACNE){
                        MPI_Send(&fedon, 2, MPI_INT, leader_id, UPLOAD_ACK, MPI_COMM_WORLD);

                    }else if(status.MPI_TAG == RETRIEVE && rank!=fedon[0]){
                       MPI_Send(&fedon, 3, MPI_INT, l_neighbor, RETRIEVE, MPI_COMM_WORLD);
                    }
                    else if(status.MPI_TAG == RETRIEVE && rank==fedon[0]){
                    //save to local
                        struct catalogue * head_ref = chead;
                        //printf("exw\n");
                        if(head_ref->data == fedon[1])fedon[2] = head_ref->version;
                        else fedon[2]=0;
                        //printf("%d\n",chead->next->version);
                        //printf("eixa\n");
                        //fedon[2] = 0;
                        MPI_Send(&fedon, 3, MPI_INT, l_neighbor, RETRIEVE_ACK, MPI_COMM_WORLD);
                    }
                    else if(status.MPI_TAG == RETRIEVE_ACK && rank!=fedon[0]){
                        MPI_Send(&fedon, 3, MPI_INT, l_neighbor, RETRIEVE_ACK, MPI_COMM_WORLD);
                    }else if(status.MPI_TAG == RETRIEVE_ACK && rank!=fedon[0] && type == LONG_ACNE) {
                        MPI_Send(&fedon, 3, MPI_INT, leader_id, RETRIEVE_ACK, MPI_COMM_WORLD);
                    }else if(status.MPI_TAG == VERSION_CHECK){
                        if(fedon[0]==1){
                            if(found)MPI_Send(&fedon, 3, MPI_INT, l_neighbor, VERSION_CHECK, MPI_COMM_WORLD);
                            else {
                                fedon[0]=0;
                                if(type == LONG_ACNE)MPI_Send(&fedon, 3, MPI_INT, leader_id, VERSION_CHECK, MPI_COMM_WORLD);
                                else MPI_Send(&fedon, 3, MPI_INT, l_neighbor, VERSION_CHECK, MPI_COMM_WORLD);
                            }
                        }else{
                            if(type == LONG_ACNE)MPI_Send(&fedon, 3, MPI_INT, leader_id, VERSION_CHECK, MPI_COMM_WORLD);
                            else MPI_Send(&fedon, 3, MPI_INT, l_neighbor, VERSION_CHECK, MPI_COMM_WORLD);
                        }
                    }
                }

            }
        } else if (status.MPI_TAG == CLIENT) {
            int type = CLIENT;
            int leader = neighbors[0];
            int file[2];
            MPI_Send(0, 0, MPI_INT, 0, ACK, MPI_COMM_WORLD);
            chead = malloc(sizeof(struct catalogue));
            chead->next = NULL;
            while(1) {
                MPI_Recv(&y, 2, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                if (status.MPI_TAG == UPLOAD && status.MPI_SOURCE == 0){
                    struct catalogue * head_ref = chead;
                    cat_insert(head_ref,y[0],1);
                    file[0] = y[0];
                    file[1] = 1;
                    MPI_Send(&file, 2, MPI_INT, leader, CLIENT_TO_LEADER, MPI_COMM_WORLD);
                 //   printf("send %d %d\n",file[0],file[1]);
                }else if(status.MPI_TAG == RETRIEVE && status.MPI_SOURCE == 0){
                    file[0] = y[0];
                    file[1] = 1;
                    MPI_Send(&file, 2, MPI_INT, leader, RETRIEVE, MPI_COMM_WORLD);
                    //MPI_Recv(&y, 2, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
                    //printf("CLIENT %d FAILED TO RETRIEVE %d\n",rank,file[0]);
                }
                //else if(status.MPI_TAG == UPLOAD_OK)printf("CLIENT %d UPLOADED %d\n",rank,file[0]);
                //else if(status.MPI_TAG == UPLOAD_FAILED)printf("CLIENT %d FAILED TO UPLOAD %d\n",rank,file[0]);

            }

        }
    }
    MPI_Finalize();
}
