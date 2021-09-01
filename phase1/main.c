#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <malloc.h>
#include <unistd.h>
int tsc=0;
int lsc=0;
int tkc,qsc,qkc=0;
int nthreads;
struct queue **array;
struct queue **carray;
int deqs=0;
pthread_barrier_t first_barrier;
pthread_barrier_t second_barrier;
pthread_barrier_t third_barrier;
pthread_mutex_t tree_lock;
pthread_mutex_t queue_lock;
struct treeNode{
    int songID;
    struct treeNode *lc;
    struct treeNode *rc;
    pthread_mutex_t lock;
}*head=NULL;
struct treeNode *chead;
struct queue{
    struct queueNode *Head;
    struct queueNode *Tail;
    pthread_mutex_t headLock;
    pthread_mutex_t taiLock;
};
struct queueNode{
    int songID;
    struct queueNode *next;
};
struct listNode{
    int songID;
    struct listNode *next;
    pthread_mutex_t lock;
};
struct list{
    struct listNode *head;
    struct listNode *tail;
};
struct list *LList;
void LL_insert(struct treeNode * t){
    struct listNode* current;
    struct listNode* new = (struct listNode *)malloc(sizeof(struct listNode));
    new->next=NULL;
    new->songID=t->songID;
    if (LList->head == NULL || LList->head->songID >= new->songID)
    {
        new->next = LList->head;
        LList->head = new;
    }
    else
    {

        current = LList->head;
        while (current->next!=NULL &&
               current->next->songID < new->songID)
        {
            current = current->next;
        }
        new->next = current->next;
        current->next = new;
    }
}
void enq(int index,int value){
    struct queueNode *n = (struct queueNode*)malloc(sizeof(struct queueNode));
    n->next=NULL;
    if(array[index]==NULL){
        array[index] = (struct queue*)malloc(sizeof(struct queue));
        array[index]->Head=(struct queueNode*)malloc(sizeof(struct queueNode));
        array[index]->Tail=(struct queueNode*)malloc(sizeof(struct queueNode));
        array[index]->Head=n;
        array[index]->Tail=n;
    }
    qsc++;
    qkc+=value;
    n->songID=value;
    pthread_mutex_lock(&array[index]->taiLock);
    array[index]->Tail->next = n;
    array[index]->Tail = n;
    pthread_mutex_unlock(&array[index]->taiLock);
}
int deq(int index){
    int result;
    pthread_mutex_lock(&array[index]->headLock);
    if(array[index]->Head->next == NULL)result=-1;
    else{

        result = array[index]->Head->next->songID;
        array[index]->Head=array[index]->Head->next;
    }

    pthread_mutex_unlock(&array[index]->headLock);
    return result;

}
struct treeNode * minValueNode(struct treeNode* node)
{
    struct treeNode* current = node;

    /* loop down to find the leftmost leaf */
    while (current && current->lc != NULL)
        current = current->lc;

    return current;
}
struct treeNode *newBSTnode(int val){
    struct treeNode *s =(struct treeNode *)malloc(sizeof(struct treeNode));
    s->songID=val;
    s->lc=NULL;
    s->rc=NULL;
    return s;
}
struct treeNode *inSucc(struct treeNode *ptr)
{
    struct treeNode *succ;
    if(ptr->rc==NULL)
        succ=ptr->rc;
    else
    {
        ptr=ptr->rc;
        while(ptr->lc!=NULL)
            ptr=ptr->lc;
        succ=ptr;
    }
    return succ;
}
struct treeNode *inPred(struct treeNode *ptr)
{
    struct treeNode *pred;
    if(ptr->lc==NULL)
        pred=ptr->lc;
    else
    {
        ptr=ptr->lc;
        while(ptr->rc!=NULL)
            ptr=ptr->rc;
        pred=ptr;
    }
    return pred;
}
struct treeNode* nochild(struct treeNode * root,struct treeNode *par,struct treeNode*ptr){
    if(par == NULL){
        root == NULL;
        printf("root == null\n");
    }
    else if(ptr == par->lc){
        pthread_mutex_lock(&par->lc->lock);
        par->lc = ptr->lc;

    }
    else{
        pthread_mutex_lock(&par->rc->lock);
        par->rc = ptr->rc;

    }
    //free(ptr);
    return par;
}
struct treeNode* onlyone(struct treeNode * root,struct treeNode*par,struct treeNode *ptr){
    struct treeNode *child;

    if(ptr->rc == NULL)child = ptr->lc;
    else child = ptr->rc;

    if(par==NULL)root = child;

    else if( ptr == par->lc)par->lc = child;
    else par->rc = child;
    struct treeNode * s,*p ;
    s=newBSTnode(-1);
    p=newBSTnode(-1);
    s=inSucc(ptr);
    p=inPred(ptr);

    if(ptr->lc!=NULL)p->rc = s;
    else{
        if(ptr->rc!=NULL)s->lc = p;

    }

    //free(ptr);
    return root;
}
struct treeNode* both(struct treeNode * root,struct treeNode*par,struct treeNode *ptr){
    struct treeNode *parsucc = ptr;
    struct treeNode *succ = ptr->rc;
    while(succ!=NULL && succ->lc!=NULL){
        parsucc = succ;
        succ = succ->lc;
    }

    if(ptr!=NULL && succ!=NULL)ptr->songID = succ->songID;
    if(succ!=NULL && succ->lc!=NULL && succ->rc!=NULL)root = nochild(root,parsucc,succ);
    else root = onlyone(root,parsucc,succ);
    //free(ptr);
    return root;
}
struct treeNode* BSTdelete(struct treeNode* root, int key){
    struct treeNode *par=(struct treeNode *)malloc(sizeof(struct treeNode));
    pthread_mutex_lock(&tree_lock);

    int found=0;
    if (head == NULL) {
        pthread_mutex_unlock(&tree_lock);
        return root;
    }
    pthread_mutex_lock(&root->lock);
    pthread_mutex_unlock(&tree_lock);
    struct treeNode* curr = head;
    struct treeNode* next = NULL;

    while (1) {
        if (key == curr->songID) {
                found =1;
                pthread_mutex_unlock(&curr->lock);
                break;
            }
        par=curr;
        if (key < curr->songID) {
            if (curr->lc == NULL) {
                pthread_mutex_unlock(&curr->lock);
                return curr;
            }else {
                next = curr->lc;
            }
        } else if (key > curr->songID) {
            if (curr->rc == NULL) {
                pthread_mutex_unlock(&curr->lock);
                return curr;
            } else {
                next = curr->rc;
            }
        } else {
            if(found==0)printf("key not found\n");
            pthread_mutex_unlock(&curr->lock);
            return root;
        }
        pthread_mutex_lock(&next->lock);
        pthread_mutex_unlock(&curr->lock);
        curr = next;
    }

    if(par==NULL || par->lc==NULL )return root;
    if(curr->rc!=NULL && curr->lc!= NULL)root=both(root,par,curr);
    else if( curr->lc!= NULL)root=onlyone(root,par,curr);
    else if(curr->rc!=NULL && curr->lc!= NULL)root=onlyone(root,par,curr);
    else root = nochild(root,par,curr);
    return root;



}
void tree_check(struct treeNode *ptr)
{
    if(ptr->lc!=NULL)tree_check(ptr->lc);
    tsc++;
    tkc+=ptr->songID;
    if(ptr->rc!=NULL)tree_check(ptr->rc);
}
void queue_check(struct queue ** array) {
    int i ;
    for (i = 0; i < nthreads / 2; i++) {
        while (array[i]->Head != NULL) {
            qsc++;
            qkc += array[i]->Head->songID;
            array[i]->Head = array[i]->Head->next;
        }
    }
}
void list_check(struct list *l){

        struct listNode *temp = l->head;
        while(temp != NULL)
        {
            lsc++;
            temp = temp->next;
        }

}
int BSTsearch(struct treeNode *root,int key){
    if (root == NULL || root->songID == key)
        return key;
    if (root->songID < key)
        return BSTsearch(root->rc, key);
    return BSTsearch(root->lc, key);;

}
int BSTinsert(int key) {
    pthread_mutex_lock(&tree_lock);
    if (head == NULL) {
        head = newBSTnode(key);
        pthread_mutex_unlock(&tree_lock);
        return 1;
    }

    pthread_mutex_lock(&head->lock);
    pthread_mutex_unlock(&tree_lock);

    struct treeNode* curr = head;
    struct treeNode* next = NULL;

    while (1) {
        if (key < curr->songID) {
            if (curr->lc == NULL) {
                curr->lc = newBSTnode(key);
                pthread_mutex_unlock(&curr->lock);
                return 1;
            }else {
                next = curr->lc;
            }
        } else if (key > curr->songID) {
            if (curr->rc == NULL) {
                curr->rc = newBSTnode(key);
                pthread_mutex_unlock(&curr->lock);
                return 1;
            } else {
                next = curr->rc;
            }
        } else {
            pthread_mutex_unlock(&curr->lock);
            return 0;
        }
        pthread_mutex_lock(&next->lock);
        pthread_mutex_unlock(&curr->lock);
        curr = next;
    }
}
void *work(void *arg){
    long tid = (long)arg;
    long songid = 0;
    int i,index,result;
    for(i=0;i<nthreads;i++){
        BSTinsert(i*nthreads+tid);
    }
    pthread_barrier_wait(&first_barrier);
    struct treeNode * t = head;
    if(tid==0)tree_check(t);

    index = ((int)tid + 1) % (nthreads/2);
    for(i = (nthreads * tid); i <= ((nthreads *tid) + (nthreads - 1)); i++) {
        result = BSTsearch(head,i);
        printf("%d\n",(index % (nthreads/2)));
        enq((index % (nthreads/2)), result);
        index++;

    }

    pthread_barrier_wait(&second_barrier);

    index = (((int)tid + 1) % (nthreads/2));
    int p;
    for(i = 0; i < (nthreads/2)-1; i++) {
        p=deq(index % (nthreads/2));
        if(p!=-1)LL_insert(BSTdelete(head,p));
    }
    pthread_barrier_wait(&third_barrier);




}
int main(int argc, char *argv[] ) {
    nthreads = atoi(argv[1]);
    pthread_t threads[atoi(argv[1])];
    int i;
    int N = atoi(argv[1]);
    long t;
    pthread_barrier_init(&first_barrier,NULL,nthreads);
    pthread_barrier_init(&second_barrier,NULL,nthreads);
    pthread_barrier_init(&third_barrier,NULL,nthreads);
    array = malloc(sizeof(struct queue) * nthreads);
    carray = malloc(sizeof(struct queue) * nthreads);
    LList = (struct list *)malloc(sizeof(struct list));
    LList->head=NULL;
    LList->tail=NULL;
    for(t=0;t<nthreads;t++){
        pthread_create(&threads[t],NULL,work,(void *)t);
    }
    int j=0;
    for(j=0;j<nthreads;j++)pthread_join(threads[j],NULL);
    printf("total size check passed(expected: %d ,found: %d)\n",nthreads*nthreads,tsc);
    printf("total keysum check passed(expected: %d ,found: %d)\n\n",nthreads*nthreads*(nthreads-1)*(nthreads+1)/2,tkc);


    printf("queue's total size check passed(expected: %d ,found: %d)\n",nthreads*nthreads,qsc);
    printf("total keysum check passed(expected: %d ,found: %d)\n\n",nthreads*nthreads*(nthreads-1)*(nthreads+1)/2,qkc);
    tsc=0;
    qsc=0;
    tree_check(head);
    queue_check(array);
    list_check(LList);
    printf("queue's total size check passed(expected: %d ,found: %d)\n",nthreads*nthreads/2,tsc);
    printf("total keysum check passed(expected: %d ,found: %d)\n",nthreads*nthreads/2,qsc);
    printf("list's size check passed(expected: %d , found: %d\n",(nthreads*nthreads)/2,lsc);

    pthread_exit(NULL);
    return 0;
}
