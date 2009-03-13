/* The code here does not have to be thread safe, it is synchronized from the
   Java side.
   */

#include <mpi.h>
#include <jni.h>
#include <stdlib.h>

#define DEBUG 0

#define TYPE_BOOLEAN 1
#define TYPE_BYTE 2
#define TYPE_CHAR 3
#define TYPE_SHORT 4
#define TYPE_INT 5
#define TYPE_FLOAT 6
#define TYPE_LONG 7
#define TYPE_DOUBLE 8
#define TYPE_COUNT 9

static int ibisMPI_rank;
static int ibisMPI_size;
static int* ibisMPI_typeSize;

static int ibisMPI_currentId;

static int noncopying = 1;

struct ibisMPI_request {
    int id;
    int isSend; /* 1 means send, 0 means recv */
    int tested; /* 1 means: already tested OK. */
    void* buffer;
    int size;
};

struct ibisMPI_buf {
    int size;
    struct ibisMPI_buf *next;
    void *buf;
};

static struct ibisMPI_buf *ibisMPI_bufcache;

struct ibisMPI_request* ibisMPI_requestInfoArray;
static MPI_Request *ibisMPI_requestArray;
static int ibisMPI_requestArraySize;
static int ibisMPI_requestsUsed;


/*
 * Class:     ibis_mpi_IbisMPI
 * Method:    init
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_init
(JNIEnv *env, jobject jthis) {
    int argc = 0;
    char* a = "";
    char** argv = &a;

    int res = MPI_Init(&argc,&argv);
    if(res != MPI_SUCCESS) return -1;

    res = MPI_Comm_rank(MPI_COMM_WORLD,&ibisMPI_rank);
    if(res != MPI_SUCCESS) return -1;

    res = MPI_Comm_size(MPI_COMM_WORLD,&ibisMPI_size);
    if(res != MPI_SUCCESS) return -1;

    ibisMPI_typeSize = (int*) malloc(TYPE_COUNT * sizeof(int));
    if(ibisMPI_typeSize == NULL) return -1;

    ibisMPI_typeSize[TYPE_BOOLEAN] = 1;
    ibisMPI_typeSize[TYPE_BYTE] = 1;
    ibisMPI_typeSize[TYPE_CHAR] = 2;
    ibisMPI_typeSize[TYPE_SHORT] = 2;
    ibisMPI_typeSize[TYPE_INT] = 4;
    ibisMPI_typeSize[TYPE_FLOAT] = 4;
    ibisMPI_typeSize[TYPE_LONG] = 8;
    ibisMPI_typeSize[TYPE_DOUBLE] = 8;

    ibisMPI_currentId = 0;

    ibisMPI_requestArraySize = 32;
    ibisMPI_requestArray = (MPI_Request *)
	malloc(ibisMPI_requestArraySize * sizeof(MPI_Request));
    ibisMPI_requestInfoArray = (struct ibisMPI_request *)
	malloc(ibisMPI_requestArraySize * sizeof(struct ibisMPI_request));
    if (ibisMPI_requestArray == NULL || ibisMPI_requestInfoArray == NULL) {
#if DEBUG       
        fprintf(stderr, "%d: MPI init: malloc failed\n", ibisMPI_rank);
#endif
        return -1;
    }
    ibisMPI_requestsUsed = 0;

#if DEBUG	
    fprintf(stderr, "%d: MPI init done\n", ibisMPI_rank);
#endif

    return 1;
}

static struct ibisMPI_buf *getBuf(int sz) {
    struct ibisMPI_buf *p = ibisMPI_bufcache;
    if (p == NULL) {
	p = (struct ibisMPI_buf *) malloc(sizeof(struct ibisMPI_buf));
	if (p == NULL) {
	    return NULL;
	}
	p->next = NULL;
	p->size = 16;
	while (p->size < sz) p->size <<= 1;
	p->buf = malloc(p->size);
	if (p->buf == NULL) {
	    return NULL;
	}
	return p;
    }

    ibisMPI_bufcache = p->next;

    while (p->size < sz) p->size <<= 1;
    p->buf = realloc(p->buf, p->size);
    if (p->buf == NULL) {
	return NULL;
    }
    return p;
}

static void releaseBuf(struct ibisMPI_buf *p) {
    p->next = ibisMPI_bufcache;
    ibisMPI_bufcache = p;
}

static struct ibisMPI_request *getCachedRequest() {
    if (ibisMPI_requestsUsed >= ibisMPI_requestArraySize) {
	ibisMPI_requestArraySize += ibisMPI_requestArraySize;
	ibisMPI_requestArray = (MPI_Request *)
	    realloc(ibisMPI_requestArray,
		    ibisMPI_requestArraySize * sizeof(MPI_Request));
	ibisMPI_requestInfoArray = (struct ibisMPI_request *)
	    realloc(ibisMPI_requestInfoArray,
		    ibisMPI_requestArraySize * sizeof(struct ibisMPI_request));
	if (ibisMPI_requestArray == NULL || ibisMPI_requestInfoArray == NULL) {
	    return NULL;
	}
    }
    return &ibisMPI_requestInfoArray[ibisMPI_requestsUsed++];
}

static void freeCachedRequest(int index) {
    ibisMPI_requestsUsed--;
    if (index < ibisMPI_requestsUsed) {
	ibisMPI_requestArray[index] = ibisMPI_requestArray[ibisMPI_requestsUsed];
	ibisMPI_requestInfoArray[index] = ibisMPI_requestInfoArray[ibisMPI_requestsUsed];
    }
}


static struct ibisMPI_request* allocRequest(void* buffer) {
    struct ibisMPI_request* res = getCachedRequest();
    if (res == NULL) {
	return NULL;
    }

    res->id = ibisMPI_currentId++;
    res->buffer = buffer;
    res->size = 0;
    res->tested = 0;
    return res;
}

static struct ibisMPI_request* findRequest(int id) {
    int i;
    struct ibisMPI_request* res;

    for (i = 0, res = &ibisMPI_requestInfoArray[0];
	    i < ibisMPI_requestsUsed; i++, res++) {
	if (res->id == id) {
	    return res;
	}
    }

    fprintf(stderr, "request not found!\n");
    return NULL;
}

static void deleteRequest(struct ibisMPI_request* toDelete) {
    freeCachedRequest(toDelete - &ibisMPI_requestInfoArray[0]);
}

JNIEXPORT void JNICALL Java_ibis_impl_mpi_IbisMPIInterface_end
(JNIEnv *env, jobject jthis) {

#if DEBUG	
    fprintf(stderr, "%d: MPI end\n", ibisMPI_rank);
#endif

    /* Note:  MPI_Finalize() does not cause an exit() */ 
    MPI_Finalize();

#if DEBUG	
    fprintf(stderr, "%d: MPI end done\n", ibisMPI_rank);
#endif
}

/*
 * Class:     ibis_mpi_IbisMPI
 * Method:    size
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_size
(JNIEnv *env, jobject jthis) {
    return ibisMPI_size;
}

/*
 * Class:     ibis_mpi_IbisMPI
 * Method:    rank
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_rank
(JNIEnv *env, jobject jthis) {
    return ibisMPI_rank;
}


static void freeSendBuffer(JNIEnv *env, jobject buf, jint type, void* bufptr) {
    if (noncopying) {
	switch(type) {
	    case TYPE_BOOLEAN:
		(*env)->ReleaseBooleanArrayElements(env, buf, bufptr, JNI_ABORT);
		break;
	    case TYPE_BYTE:
		(*env)->ReleaseByteArrayElements(env, buf, bufptr, JNI_ABORT);
		break;
	    case TYPE_CHAR:
		(*env)->ReleaseCharArrayElements(env, buf, bufptr, JNI_ABORT);
		break;
	    case TYPE_SHORT:
		(*env)->ReleaseShortArrayElements(env, buf, bufptr, JNI_ABORT);
		break;
	    case TYPE_INT:
		(*env)->ReleaseIntArrayElements(env, buf, bufptr, JNI_ABORT);
		break;
	    case TYPE_FLOAT:
		(*env)->ReleaseFloatArrayElements(env, buf, bufptr, JNI_ABORT);
		break;
	    case TYPE_LONG:
		(*env)->ReleaseLongArrayElements(env, buf, bufptr, JNI_ABORT);
		break;
	    case TYPE_DOUBLE:
		(*env)->ReleaseDoubleArrayElements(env, buf, bufptr, JNI_ABORT);
		break;
	    default:
		fprintf(stderr, "unknown type: %d\n", type);
		break;
	}
    } else {
	free(bufptr);
    }    
}


static void* getWholeBuffer(JNIEnv *env, jobject buf, jint type) {
    jboolean isCopy = -1;
    void *b = NULL;

    switch(type) {
	case TYPE_BOOLEAN:
	    b = (*env)->GetBooleanArrayElements(env, buf, &isCopy);
	    break;
	case TYPE_BYTE:
	    b = (*env)->GetByteArrayElements(env, buf, &isCopy);
	    break;
	case TYPE_CHAR:
	    b = (*env)->GetCharArrayElements(env, buf, &isCopy);
	    break;
	case TYPE_SHORT:
	    b = (*env)->GetShortArrayElements(env, buf, &isCopy);
	    break;
	case TYPE_INT:
	    b = (*env)->GetIntArrayElements(env, buf, &isCopy);
	    break;
	case TYPE_FLOAT:
	    b = (*env)->GetFloatArrayElements(env, buf, &isCopy);
	    break;
	case TYPE_LONG:
	    b = (*env)->GetLongArrayElements(env, buf, &isCopy);
	    break;
	case TYPE_DOUBLE:
	    b = (*env)->GetDoubleArrayElements(env, buf, &isCopy);
	    break;
	default:
	    fprintf(stderr, "unknown type: %d\n", type);
	    return NULL;
    }
    if (isCopy == JNI_TRUE) {
	freeSendBuffer(env, buf, type, b);
	b = NULL;
	noncopying = 0;
    }
    return b;
}

static void* getPartialBuffer(JNIEnv *env, jobject buf, jint offset, jint count,
	jint type) {
    int size = count * ibisMPI_typeSize[type];
    void *bufptr = malloc(size);
    if(bufptr == NULL) return NULL;

    switch(type) {
	case TYPE_BOOLEAN:
	    (*env)->GetBooleanArrayRegion(env, buf, offset, count, bufptr);
	    break;
	case TYPE_BYTE:
	    (*env)->GetByteArrayRegion(env, buf, offset, count, bufptr);
	    break;
	case TYPE_CHAR:
	    (*env)->GetCharArrayRegion(env, buf, offset, count, bufptr);
	    break;
	case TYPE_SHORT:
	    (*env)->GetShortArrayRegion(env, buf, offset, count, bufptr);
	    break;
	case TYPE_INT:
	    (*env)->GetIntArrayRegion(env, buf, offset, count, bufptr);
	    break;
	case TYPE_FLOAT:
	    (*env)->GetFloatArrayRegion(env, buf, offset, count, bufptr);
	    break;
	case TYPE_LONG:
	    (*env)->GetLongArrayRegion(env, buf, offset, count, bufptr);
	    break;
	case TYPE_DOUBLE:
	    (*env)->GetDoubleArrayRegion(env, buf, offset, count, bufptr);
	    break;
	default:
	    fprintf(stderr, "unknown type: %d\n", type);
	    return NULL;
    }
    return bufptr;
}


static void* getSendBuffer(JNIEnv *env, jobject buf, jint offset, jint count,
	jint type) {

    if (noncopying) {
	void *p = getWholeBuffer(env, buf, type);
	if (noncopying) {
	    return p;
	}
    }
    return getPartialBuffer(env, buf, offset, count, type);
}


static struct ibisMPI_buf *getRcvBuffer(JNIEnv *env, jobject buf, jint offset,
	jint count, jint type) {
    int size = count * ibisMPI_typeSize[type];
    return getBuf(size);
}


static void freeRcvBuffer(JNIEnv *env, jobject buf, jint offset, jint count,
	jint type, void* bufptr) {

    struct ibisMPI_buf *p = (struct ibisMPI_buf *) bufptr;

    switch(type) {
	case TYPE_BOOLEAN:
	    (*env)->SetBooleanArrayRegion(env, buf, offset, count, p->buf);
	    break;
	case TYPE_BYTE:
	    (*env)->SetByteArrayRegion(env, buf, offset, count, p->buf);
	    break;
	case TYPE_CHAR:
	    (*env)->SetCharArrayRegion(env, buf, offset, count, p->buf);
	    break;
	case TYPE_SHORT:
	    (*env)->SetShortArrayRegion(env, buf, offset, count, p->buf);
	    break;
	case TYPE_INT:
	    (*env)->SetIntArrayRegion(env, buf, offset, count, p->buf);
	    break;
	case TYPE_FLOAT:
	    (*env)->SetFloatArrayRegion(env, buf, offset, count, p->buf);
	    break;
	case TYPE_LONG:
	    (*env)->SetLongArrayRegion(env, buf, offset, count, p->buf);
	    break;
	case TYPE_DOUBLE:
	    (*env)->SetDoubleArrayRegion(env, buf, offset, count, p->buf);
	    break;
	default:
	    fprintf(stderr, "unknown type: %d\n", type);
	    break;
    }
    releaseBuf(p);
}


JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_send(JNIEnv *env, jobject jthis,
	jobject buf, jint offset, jint count,
	jint type, jint dest, jint tag) {
    void* bufptr;
    int size;
    int res;

    size = count * ibisMPI_typeSize[type];
    bufptr = getSendBuffer(env, buf, offset, count, type);
    if(bufptr == NULL) return -1;

#if DEBUG	
    fprintf(stderr, "%d: send of %d bytes to %d, tag is %d, type is %d (%d bytes/elt)\n", 
	    ibisMPI_rank, size, dest, tag, type, ibisMPI_typeSize[type]);
#endif

    if (noncopying) {
	res = MPI_Send(bufptr + offset*ibisMPI_typeSize[type], size, MPI_BYTE, dest, tag, MPI_COMM_WORLD);
    } else {
	res = MPI_Send(bufptr, size, MPI_BYTE, dest, tag, MPI_COMM_WORLD);
    }

#if DEBUG	
    fprintf(stderr, "%d: send of %d bytes to %d, tag is %d, type is %d (%d bytes/elt) DONE\n", 
	    ibisMPI_rank, size, dest, tag, type, ibisMPI_typeSize[type]);
#endif

    freeSendBuffer(env, buf, type, bufptr);

    if(res != MPI_SUCCESS) return -1;

    return 1;
}

JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_recv(JNIEnv *env,
	jobject jthis, jobject buf, jint offset, jint count,
	jint type, jint src, jint tag) {
    struct ibisMPI_buf * bufptr;
    int size;
    int res;
    MPI_Status status;

    size = count * ibisMPI_typeSize[type];
    bufptr = getRcvBuffer(env, buf, offset, count, type);
    if(bufptr == NULL) return -1;

#if DEBUG
    fprintf(stderr, "%d: recv of %d bytes from %d, tag is %d\n", ibisMPI_rank, size, src, tag);
#endif

    res = MPI_Recv(bufptr->buf, size, MPI_BYTE, src, tag, MPI_COMM_WORLD, &status);

#if DEBUG
    fprintf(stderr, "%d: recv of %d bytes from %d, tag is %d DONE\n", ibisMPI_rank, size, src, tag);
#endif

    int bytecount;

    res = MPI_Get_count(&status, MPI_BYTE, &bytecount);

    freeRcvBuffer(env, buf, offset, count, type, bufptr);

    if(res != MPI_SUCCESS) return -1;

    return bytecount;
}

JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_isend(JNIEnv *env,
	jobject jthis, jobject buf, jint offset, jint count,
	jint type, jint dest, jint tag) {
    void* bufptr;
    int size;
    int res;

    size = count * ibisMPI_typeSize[type];
    bufptr = getSendBuffer(env, buf, offset, count, type);
    if(bufptr == NULL) return -1;

    struct ibisMPI_request* req = allocRequest(bufptr);
    req->isSend = 1;

    int index = req - &ibisMPI_requestInfoArray[0];

#if DEBUG
    fprintf(stderr, "%d: Isend of %d bytes to %d, tag is %d, type is %d (%d bytes/elt) ID = %d\n", 
	    ibisMPI_rank, size, dest, tag, type, ibisMPI_typeSize[type], req->id);
#endif

    if (noncopying) {
	res = MPI_Isend(bufptr + offset*ibisMPI_typeSize[type], size, MPI_BYTE, dest, tag, MPI_COMM_WORLD, &ibisMPI_requestArray[index]);
    } else {
	res = MPI_Isend(bufptr, size, MPI_BYTE, dest, tag, MPI_COMM_WORLD, &ibisMPI_requestArray[index]);
    }

#if DEBUG
    fprintf(stderr, "%d: Isend of %d bytes to %d, tag is %d, type is %d (%d bytes/elt) ID = %d DONE\n", 
	    ibisMPI_rank, size, dest, tag, type, ibisMPI_typeSize[type], req->id);
#endif

    if(res != MPI_SUCCESS) return -1;

    return req->id;
}

JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_irecv(JNIEnv *env,
	jobject jthis, jobject buf, jint offset, jint count, jint type,
	jint src, jint tag) {
    struct ibisMPI_buf *bufptr;
    int size;
    int res;

    size = count * ibisMPI_typeSize[type];
    bufptr = getRcvBuffer(env, buf, offset, count, type);
    if(bufptr == NULL) return -1;

    struct ibisMPI_request* req = allocRequest(bufptr);
    req->isSend = 0;

    int index = req - &ibisMPI_requestInfoArray[0];

#if DEBUG
    fprintf(stderr, "%d: Irecv of %d bytes from %d, tag is %d, type is %d (%d bytes/elt) ID = %d\n", 
	    ibisMPI_rank, size, src, tag, type, ibisMPI_typeSize[type], req->id);
#endif

    ((char *)bufptr->buf)[0] = 33;
    res = MPI_Irecv(bufptr->buf, size, MPI_BYTE, src, tag, MPI_COMM_WORLD, &ibisMPI_requestArray[index]);

#if DEBUG
    fprintf(stderr, "%d: Irecv of %d bytes from %d, tag is %d, type is %d (%d bytes/elt) ID = %d DONE, SUCCESS = %d\n", 
	    ibisMPI_rank, size, src, tag, type, ibisMPI_typeSize[type], req->id, res == MPI_SUCCESS);
#endif

    if(res != MPI_SUCCESS) return -1;

    return req->id;
}

JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_test
(JNIEnv *env, jobject jthis, jint id, jobject buf, jint offset, jint count,
 jint type) {
    int res;
    int flag;
    MPI_Status status;
    struct ibisMPI_request* req = findRequest(id);

    if (! req->tested) {
	int index = req - &ibisMPI_requestInfoArray[0];
	res = MPI_Test(&ibisMPI_requestArray[index], &flag, &status);
#if DEBUG
	fprintf(stderr, "%d: test for id %d, flag = %d\n", ibisMPI_rank, id, flag);
#endif
	if (flag) { /* send or receive is done */
	    req->tested = 1;
	    if (! req->isSend) {
		MPI_Get_count(&status, MPI_BYTE, &(req->size));
#if DEBUG
		fprintf(stderr, "%d: in test, get_count for id %d, result = %d\n", ibisMPI_rank, id, req->size);
#endif
	    }
	}
    } else {
	flag = 1;
    }

    if(flag) { /* send or receive is done */
	int size = req->size;
	if (req->isSend) {
	    freeSendBuffer(env, buf, type, req->buffer);
	} else {
	    freeRcvBuffer(env, buf, offset, count, type, req->buffer);
	}
	deleteRequest(req);
	return size;
    }

    return -1;
}

JNIEXPORT jint JNICALL Java_ibis_impl_mpi_IbisMPIInterface_testAny
(JNIEnv *env, jobject jthis, jint id) {
    int res;
    int flag;
    int index = -1;
    MPI_Status status;

    /* First test my own id. */
    struct ibisMPI_request* req = findRequest(id);
    if (req->tested) {
	return id;
    }

    res = MPI_Testany(ibisMPI_requestsUsed, ibisMPI_requestArray,
	    &index, &flag, &status);

    if (index < 0) {
	return -1;
    }

    req = &ibisMPI_requestInfoArray[index];

    if (! req->isSend) {
	MPI_Get_count(&status, MPI_BYTE, &(req->size));
#if DEBUG
	fprintf(stderr, "%d: in testAny, get_count for id %d, result = %d\n", ibisMPI_rank, req->id, req->size);
#endif
    } else {
#if DEBUG
	fprintf(stderr, "%d: in testAny, send id %d\n", ibisMPI_rank, req->id);
#endif
    }
    req->tested = 1;
    return req->id;
}
