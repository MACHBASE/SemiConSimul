/******************************************************************************
 * Copyright of this product 2013-2023,
 * MACHBASE Corporation(or Inc.) or its subsidiaries.
 * All Rights reserved.
 ******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <machbase_sqlcli.h>

#define SPARAM_MAX_COLUMN   4
#define ERROR_CHECK_COUNT	100000
#define RC_SUCCESS         	0
#define RC_FAILURE         -1

#define PORT_NO             5656

#define UNUSED(aVar) do { (void)(aVar); } while(0)

#define CHECK_APPEND_RESULT(aRC, aEnv, aCon, aSTMT)             \
    if( !SQL_SUCCEEDED(aRC) )                                   \
    {                                                           \
        if( checkAppendError(aEnv, aCon, aSTMT) == RC_FAILURE ) \
        {                                                       \
           return RC_FAILURE;                                   \
        }                                                       \
    }


typedef struct tm timestruct;

SQLHENV 	gEnv;
SQLHDBC 	gCon;
SQLHDBC     gLotDataConn;

static char          gTargetIP[16];
static int           gPortNo=PORT_NO;
static unsigned long gMaxData=0;
static long          gTps=3000000;
static char          gTable[64]="TAG";
static int           gEquipCnt;
static int           gTagPerEq;
static int           gDataPerSec;
static long          gLotProcessTime;
int                  gNoLotNo=0;

static char *gEnvVarNames[] = { "TEST_EQUIP_CNT",
                                "TEST_TAG_PER_EQ",
                                "TEST_LOT_PROCESS_TIME",
                                "TEST_MAX_ROWCNT",
                                "TEST_DATA_TAG_PER_SEC",
                                "TEST_TARGET_EPS",
                                "TEST_PORT_NO",
                                "TEST_SERVER_IP",
                                 NULL
};

struct eq_lot_info
{
    long mLastTime;
    int  mLastLot;
};

time_t getTimeStamp();
void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
int connectDB();
void disconnectDB();
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt);
unsigned long appendClose(SQLHSTMT aStmt);

int init_eq_lot_info(struct eq_lot_info *aInfo, long aTime, int aEqCnt)
{
    aInfo->mLastTime = aTime;
    aInfo->mLastLot  = aEqCnt;
    return 0;
}

time_t getTimeStamp()
{
    struct timeval sTimeVal;
    int            sRet;

    sRet = gettimeofday(&sTimeVal, NULL);

    if (sRet == 0)
    {
        return (time_t)(sTimeVal.tv_sec * 1000000 + sTimeVal.tv_usec);
    }
    else
    {
        return 0;
    }
}

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( aMsg != NULL )
    {
        printf("%s\n", aMsg);
    }

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS )
    {
        printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);
    }
}

int checkAppendError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS )
    {
        return RC_FAILURE;
    }

    printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);

    if( sNativeError != 9604 &&
        sNativeError != 9605 &&
        sNativeError != 9606 )
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void appendDumpError(SQLHSTMT    aStmt,
                 SQLINTEGER  aErrorCode,
                 SQLPOINTER  aErrorMessage,
                 SQLLEN      aErrorBufLen,
                 SQLPOINTER  aRowBuf,
                 SQLLEN      aRowBufLen)
{
    char       sErrMsg[1024] = {0, };
    char       sRowMsg[32 * 1024] = {0, };

    UNUSED(aStmt);

    if (aErrorMessage != NULL)
    {
        strncpy(sErrMsg, (char *)aErrorMessage, aErrorBufLen);
    }

    if (aRowBuf != NULL)
    {
        strncpy(sRowMsg, (char *)aRowBuf, aRowBufLen);
    }

    fprintf(stdout, "Append Error : [%d][%s]\n[%s]\n\n", aErrorCode, sErrMsg, sRowMsg);
}


int connectDB()
{
    char sConnStr[1024];

    if( SQLAllocEnv(&gEnv) != SQL_SUCCESS ) 
    {
        printf("SQLAllocEnv error\n");
        return RC_FAILURE;
    }

    if( SQLAllocConnect(gEnv, &gCon) != SQL_SUCCESS ) 
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf(sConnStr,"SERVER=%s;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", gTargetIP, gPortNo);

    if( SQLDriverConnect( gCon, NULL,
                          (SQLCHAR *)sConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {

        printError(gEnv, gCon, NULL, "SQLDriverConnect error");

        SQLFreeConnect(gCon);
        gCon = SQL_NULL_HDBC;

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

int connectOther()
{
    char sConnStr[1024];

    if( SQLAllocConnect(gEnv, &gLotDataConn) != SQL_SUCCESS ) 
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf(sConnStr,"SERVER=%s;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", gTargetIP, gPortNo);

    if( SQLDriverConnect( gLotDataConn, NULL,
                          (SQLCHAR *)sConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {

        printError(gEnv, gLotDataConn, NULL, "SQLDriverConnect error");

        SQLFreeConnect(gLotDataConn);
        gCon = SQL_NULL_HDBC;


        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void disconnectDB()
{
    if( SQLDisconnect(gCon) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, NULL, "SQLDisconnect error");
    }

    SQLFreeConnect(gCon);
    gCon = SQL_NULL_HDBC;

    SQLFreeEnv(gEnv);
    gEnv = SQL_NULL_HENV;
}

int appendOpen(SQLHSTMT aStmt)
{
    const char *sTableName = gTable;

    if( SQLAppendOpen(aStmt, (SQLCHAR *)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendOpen Error");
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

//#define DEBUG
void addLotEqInfo(SQLHSTMT sLotEqStmt, long aCurrentTime, struct eq_lot_info *aInfo)
{
    int              i;
    SQL_APPEND_PARAM sParam[5];
    char             sEqName[20];
    char             sLotName[20];
    
    for (i = 0; i < gEquipCnt; i++)
    {
        SQLRETURN sRC;
        sEqName[0] = 0;
        sLotName[0] = 0;
        // * LotID
        snprintf(sLotName, 20, "LOT%d", i + aInfo->mLastLot);
        sParam[0].mVar.mLength = strnlen(sLotName, 20);
        sParam[0].mVar.mData = sLotName;
        // * EqID
        snprintf(sEqName, 20, "EQ%d", i);
        sParam[1].mVar.mLength = strnlen(sEqName, 20);
        sParam[1].mVar.mData = sEqName;
        // * Begin
        sParam[2].mDateTime.mTime = aInfo->mLastTime;
        // * End
        sParam[3].mDateTime.mTime = aCurrentTime;
        // * LotNo
        sParam[4].mLong = (long)(i + aInfo->mLastLot);
        #ifdef DEBUG
        printf("LotID %s, EqID %s, Begin %ld, End %ld, LotNo %d\n",
               sLotName, sEqName, aInfo->mLastTime, aCurrentTime,
               i+aInfo->mLastLot);
        #endif
        sRC = SQLAppendDataV2(sLotEqStmt, sParam);
        CHECK_APPEND_RESULT(sRC, gEnv, gLotDataConn, sLotEqStmt);
        SQLAppendFlush(sLotEqStmt);
    }
    aInfo->mLastTime = aCurrentTime;
    aInfo->mLastLot++;
}

int getCurrentLotNo(struct eq_lot_info *aInfo,
                    int    aCurrentTagNo)
{
    int sCurrentEqNo;
    sCurrentEqNo = aCurrentTagNo/gTagPerEq;
    return sCurrentEqNo + aInfo->mLastLot;
}

void appendTps(SQLHSTMT aStmt)
{
    SQL_APPEND_PARAM *sParam;
    SQLRETURN        sRC;
    SQLHSTMT         sLotEqStmt;
    unsigned long    i;
    int j,p;
    unsigned long    sRecCount = 0;
    char	         sTagName[20];
    int              sTag;
    double           sValue;
    int               year,month,hour,min,sec,day;

    struct tm         sTm;
    long              sTime;
    long              sInterval;
    long              StartTime;
    long              sLastLotTime;
    struct eq_lot_info sInfo;
    int               sBreak = 0;

    year     =    2017;
    month    =    0;
    day      =    1;
    hour     =    0;
    min      =    0;
    sec      =    0;

    sTm.tm_year = year - 1900;
    sTm.tm_mon  = month;
    sTm.tm_mday = day;
    sTm.tm_hour = hour;
    sTm.tm_min  = min;
    sTm.tm_sec  = sec;

    sTime = mktime(&sTm);
    sTime = sTime * MACHBASE_UINT64_LITERAL(1000000000); 

    sLastLotTime = sTime;
    
    if (gNoLotNo == 0)
    {
        sParam = malloc(sizeof(SQL_APPEND_PARAM) * 4);
        memset(sParam, 0, sizeof(SQL_APPEND_PARAM) *4);
    }
    else
    {
        sParam = malloc(sizeof(SQL_APPEND_PARAM) * 3);
        memset(sParam, 0, sizeof(SQL_APPEND_PARAM)*3);
    }
    
    sInterval = (1000000000l/gDataPerSec); // 100ms default.
    
    if (SQLAllocStmt(gLotDataConn, &sLotEqStmt) != SQL_SUCCESS)
    {
        printf("AllocStmtError\n");
        exit(-1);
    }
 
    if (SQLAppendOpen(sLotEqStmt,"PROCESS_DATA", ERROR_CHECK_COUNT)
        != SQL_SUCCESS)
    {
            printf("AppendOpenError\n");
            exit(-1);
    }
    
    StartTime = getTimeStamp();
    
    init_eq_lot_info(&sInfo, sTime, gEquipCnt);
    
    for( i = 0; (gMaxData == 0) || sBreak == 0; i++ )
    {
        int sEq = 0;
    
        /* event time */
        if (sTime - sLastLotTime >= gLotProcessTime)
        {
            sLastLotTime = sTime;
            addLotEqInfo(sLotEqStmt, sLastLotTime, &sInfo);
        }
	    for( j=0; j< (gEquipCnt * gTagPerEq); j++)
        {
            /* tag_id */
            //sTag = (rand()%100);
            sTag = j;
            sTagName[0]=0;
            snprintf(sTagName, 20, "EQ%d^TAG%d",sEq, sTag);
            sParam[0].mVar.mLength   = strnlen(sTagName,20);
            sParam[0].mVar.mData     = sTagName;
            sParam[1].mDateTime.mTime =  sTime;
            if (gNoLotNo == 0)
            {
                sParam[3].mLong         = (long)getCurrentLotNo(&sInfo, j);
            }
            p = j%20;
            /* value */
            switch(p)
            {
                case 1:
                case 6:
                    sValue = ((rand()%501)*0.01)+20; //20 ~ 25;
                    break;
                case 2:
                case 7:
                    sValue = ((rand()%901)*0.01)+30; //30 ~ 39;
                    break;
                case 3:
                case 8:
                    sValue = ((rand()%1501)*0.01)+50; //50 ~ 65;
                    break;
                case 4:
                case 9:
                    sValue = ((rand()%1501)*0.01)+1000;
                    break;
                case 11:
                case 16:
                    sValue = 31.2;
                    break;
                case 12:
                case 17:
                    sValue = 234.567;
                    break;
                default:
                    sValue = (rand()%20000)/100.0; //0 ~ 200
                    break;
            }
            
            sParam[2].mDouble       = sValue;
            sRC = SQLAppendDataV2(aStmt, sParam);
            sRecCount++;
            CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);
            
            if ((gTps != 0) && (sRecCount % 10 == 0))
            {
                long usecperev = 1000000/gTps;
                long sleepus;
                long elapsed = getTimeStamp() - StartTime;
                sleepus = (usecperev * i) - elapsed;
                if (sleepus > 0)
                {
                    struct timespec sleept;
                    struct timespec leftt;
                    sleept.tv_sec = 0;
                    sleept.tv_nsec = sleepus * 1000;
                    nanosleep(&sleept, &leftt);
                }
            }
            
            if (sTag % gTagPerEq == 0 && sTag != 0)
            {
                sEq ++;
                if (sEq == gEquipCnt) sEq = 0;
            }
            
            if (sRecCount > gMaxData)
            {
                goto exit;
            }
        }
        sTime = sTime + sInterval;
    }

/*
   printf("====================================================\n");
   printf("total time : %ld sec\n", sEndTime - sStartTime);
   printf("average tps : %f \n", ((float)gMaxData/(sEndTime - sStartTime)));
   printf("====================================================\n");
*/
exit:
    return;
}

int appendData(SQLHSTMT aStmt)
{
    appendTps(aStmt);

    return RC_SUCCESS;
}

unsigned long appendClose(SQLHSTMT aStmt)
{
    unsigned long sSuccessCount = 0;
    unsigned long sFailureCount = 0;

    if( SQLAppendClose(aStmt, (SQLBIGINT *)&sSuccessCount, (SQLBIGINT *)&sFailureCount) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    printf("success : %ld, failure : %ld\n", sSuccessCount, sFailureCount);

    return sSuccessCount;
}

int SetGlobalVariables()
{
    int i = 0;
    char        *sEnvVar;

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gEquipCnt = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gTagPerEq = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gLotProcessTime = atoll(sEnvVar);
    gLotProcessTime = (long)gLotProcessTime * MACHBASE_UINT64_LITERAL(1000000000);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gMaxData = atoll(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gDataPerSec   = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gTps    = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    gPortNo  = atoi(sEnvVar);

    sEnvVar = getenv(gEnvVarNames[i]);
    if (sEnvVar == NULL)
        goto error;
    i++;
    strncpy(gTargetIP, sEnvVar, sizeof(gTargetIP));
    return 0;

    sEnvVar = getenv("NO_LOTNO");
    if (sEnvVar != NULL)
        gNoLotNo = atoi(sEnvVar);
error:
    printf("Environment variable %s was not set!\n", gEnvVarNames[i]);
    return -1;
}

int main()
{
    SQLHSTMT    sStmt = SQL_NULL_HSTMT;

    unsigned long  sCount=0;
    time_t      sStartTime, sEndTime;

    if (SetGlobalVariables() != 0)
    {
        exit(-1);
    }
    srand(time(NULL));

    if( connectDB() == RC_SUCCESS )
    {
        printf("connectDB success\n");
    }
    else
    {
        printf("connectDB failure\n");
        goto error;
    }
    connectOther();


    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS ) 
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( appendOpen(sStmt) == RC_SUCCESS )
    {
        printf("appendOpen success\n");
    }
    else
    {
        printf("appendOpen failure\n");
        goto error;
    }

    if( SQLAppendSetErrorCallback(sStmt, appendDumpError) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAppendSetErrorCallback Error");
        goto error;
    }

    sStartTime = getTimeStamp();
    appendData(sStmt);
    sEndTime = getTimeStamp();

    sCount = appendClose(sStmt);
    if( sCount > 0 )
    {
        printf("appendClose success\n");
        printf("timegap = %ld microseconds for %ld records\n", sEndTime - sStartTime, sCount);
        printf("%.2f records/second\n",  ((double)sCount/(double)(sEndTime - sStartTime))*1000000);
    }
    else
    {
        printf("appendClose failure\n");
    }

    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;

    disconnectDB();

    return RC_SUCCESS;

error:
    if( sStmt != SQL_NULL_HSTMT )
    {
        SQLFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

    if( gCon != SQL_NULL_HDBC )
    {
        disconnectDB();
    }

    return RC_FAILURE;
}
