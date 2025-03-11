#include "postgres.h"
#include "nodes/bitmapset.h"
#include "utils/relcache.h"

typedef enum CommandType
{
    EXIT,
    QUERY,
    STAT_ADVISOR,

} CommandType;

void HandleCommand(const StringInfo command_data);
bool IsDatabaseCommand(StringInfo buf, Oid *dbid, Oid *userid);

StringInfo CreateExitCommand(void);
StringInfo CreateQueryCommand(Oid dbid, Oid userid, Size sql_len, char *sql);
StringInfo CreateStatAdvisorCommand(Oid dbid, Oid userid, Oid table_id, Bitmapset *columns);
