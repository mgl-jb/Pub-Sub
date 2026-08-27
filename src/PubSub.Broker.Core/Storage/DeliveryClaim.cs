using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using PubSub.Abstractions;

namespace PubSub.Broker.Core;

/// <summary>
/// The peek-lock claim: atomically hands a batch of available deliveries to one receiver.
/// </summary>
/// <remarks>
/// <para>
/// This is the primitive the whole broker rests on, and it is a single statement on purpose. A
/// <c>SELECT</c> followed by an <c>UPDATE</c> would leave a window in which two receivers both
/// select the same row and both believe they own it; doing the selection and the lock in one
/// <c>UPDATE</c> closes it, because the row is locked from the moment it is read.
/// </para>
/// <para>
/// The hints matter individually:
/// </para>
/// <list type="bullet">
///   <item><c>UPDLOCK</c> takes the update lock at read time rather than upgrading later, which is
///   what removes the race.</item>
///   <item><c>READPAST</c> makes a receiver skip rows another receiver already holds instead of
///   blocking on them. Without it competing consumers serialise behind each other and the
///   throughput of N receivers is that of one.</item>
///   <item><c>ROWLOCK</c> discourages lock escalation to page or table level, which would block
///   unrelated subscriptions sharing the table.</item>
/// </list>
/// <para>
/// Claimed rows are written into a table variable and joined to the messages in the same batch, so
/// the receiver gets bodies and lock tokens in one round trip rather than two.
/// </para>
/// </remarks>
internal static class DeliveryClaim
{
    /// <summary>
    /// Claims up to <paramref name="maxMessages"/> available deliveries, locking each for
    /// <paramref name="lockDuration"/>.
    /// </summary>
    public static async Task<IReadOnlyList<ReceivedMessage>> ClaimAsync(
        BrokerDbContext context,
        int subscriptionId,
        int maxMessages,
        TimeSpan lockDuration,
        DateTimeOffset now,
        string? sessionId,
        string? receiverId,
        bool fromDeadLetterQueue,
        TimeSpan commandTimeout,
        CancellationToken cancellationToken)
    {
        // The dead-letter queue is not a separate table: it is the deliveries whose state is
        // DeadLettered. Reading it hands out ordinary peek-locks so a replay tool can settle
        // exactly like a normal receiver.
        MessageState claimFrom = fromDeadLetterQueue ? MessageState.DeadLettered : MessageState.Available;

        string sessionPredicate = sessionId is null
            ? string.Empty
            : "AND d.SessionId = @sessionId";

        // A dead-letter read must not reset the message's delivery count or its dead-letter
        // reason, so only the lock columns are written in that case.
        string setClause = fromDeadLetterQueue
            ? """
                  State        = State,
                  LockToken    = NEWID(),
                  LockedUntil  = @lockedUntil,
                  LockedBy     = @receiverId
              """
            : """
                  State         = 1,
                  LockToken     = NEWID(),
                  LockedUntil   = @lockedUntil,
                  LockedBy      = @receiverId,
                  DeliveryCount = DeliveryCount + 1
              """;

        string sql = $"""
            DECLARE @claimed TABLE (
                DeliveryId      bigint          NOT NULL,
                MessageSeq      bigint          NOT NULL,
                LockToken       uniqueidentifier NOT NULL,
                LockedUntil     datetimeoffset(7) NOT NULL,
                DeliveryCount   int             NOT NULL,
                OverriddenProps nvarchar(max)   NULL,
                DlqReason       nvarchar(128)   NULL,
                DlqDescription  nvarchar(2048)  NULL
            );

            WITH candidate AS (
                SELECT TOP (@maxMessages) d.*
                FROM   Deliveries AS d WITH (ROWLOCK, READPAST, UPDLOCK)
                WHERE  d.SubscriptionId = @subscriptionId
                  AND  d.State          = @claimFrom
                  AND  d.AvailableAt   <= @now
                  AND  d.ExpiresAt      > @now
                  {sessionPredicate}
                ORDER BY d.SequenceNumber
            )
            UPDATE candidate
            SET {setClause}
            OUTPUT
                inserted.Id,
                inserted.MessageSequenceNumber,
                inserted.LockToken,
                inserted.LockedUntil,
                inserted.DeliveryCount,
                inserted.OverriddenPropertiesJson,
                inserted.DeadLetterReason,
                inserted.DeadLetterDescription
            INTO @claimed;

            SELECT
                c.DeliveryId,
                c.LockToken,
                c.LockedUntil,
                c.DeliveryCount,
                c.OverriddenProps,
                c.DlqReason,
                c.DlqDescription,
                m.SequenceNumber,
                m.MessageId,
                m.CorrelationId,
                m.Subject,
                m.ContentType,
                m.Body,
                m.ApplicationPropertiesJson,
                m.SessionId,
                m.ReplyTo,
                m.ReplyToSessionId,
                m.[To],
                m.EnqueuedTime
            FROM @claimed AS c
            INNER JOIN Messages AS m ON m.SequenceNumber = c.MessageSeq
            ORDER BY m.SequenceNumber;
            """;

        DbConnection connection = context.Database.GetDbConnection();
        bool opened = false;

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
            opened = true;
        }

        try
        {
            await using DbCommand command = connection.CreateCommand();
            command.CommandText = sql;
            command.CommandTimeout = (int)commandTimeout.TotalSeconds;

            // The claim runs inside the caller's transaction when there is one, so a receive that
            // is part of a larger unit of work does not silently escape it.
            if (context.Database.CurrentTransaction is { } transaction)
            {
                command.Transaction = transaction.GetDbTransaction();
            }

            AddParameter(command, "@maxMessages", DbType.Int32, maxMessages);
            AddParameter(command, "@subscriptionId", DbType.Int32, subscriptionId);
            AddParameter(command, "@claimFrom", DbType.Int32, (int)claimFrom);
            AddParameter(command, "@now", DbType.DateTimeOffset, now);
            AddParameter(command, "@lockedUntil", DbType.DateTimeOffset, now.Add(lockDuration));
            AddParameter(command, "@receiverId", DbType.String, (object?)receiverId ?? DBNull.Value);

            if (sessionId is not null)
            {
                AddParameter(command, "@sessionId", DbType.String, sessionId);
            }

            List<ReceivedMessage> claimed = [];

            await using DbDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                claimed.Add(ReadMessage(reader, fromDeadLetterQueue));
            }

            return claimed;
        }
        finally
        {
            if (opened)
            {
                await connection.CloseAsync();
            }
        }
    }

    private static ReceivedMessage ReadMessage(DbDataReader reader, bool fromDeadLetterQueue)
    {
        long deliveryId = reader.GetInt64(0);
        Guid lockToken = reader.GetGuid(1);
        DateTimeOffset lockedUntil = reader.GetFieldValue<DateTimeOffset>(2);
        int deliveryCount = reader.GetInt32(3);
        string? overriddenProperties = reader.IsDBNull(4) ? null : reader.GetString(4);
        string? dlqReason = reader.IsDBNull(5) ? null : reader.GetString(5);
        string? dlqDescription = reader.IsDBNull(6) ? null : reader.GetString(6);

        long sequenceNumber = reader.GetInt64(7);
        string messageId = reader.GetString(8);
        string? correlationId = reader.IsDBNull(9) ? null : reader.GetString(9);
        string? subject = reader.IsDBNull(10) ? null : reader.GetString(10);
        string contentType = reader.GetString(11);
        byte[] body = (byte[])reader.GetValue(12);
        string? propertiesJson = reader.IsDBNull(13) ? null : reader.GetString(13);
        string? sessionId = reader.IsDBNull(14) ? null : reader.GetString(14);
        string? replyTo = reader.IsDBNull(15) ? null : reader.GetString(15);
        string? replyToSessionId = reader.IsDBNull(16) ? null : reader.GetString(16);
        string? to = reader.IsDBNull(17) ? null : reader.GetString(17);
        DateTimeOffset enqueuedTime = reader.GetFieldValue<DateTimeOffset>(18);

        // A rule action rewrote the properties for this subscription only; fall back to the
        // message's own properties when it did not, which is the common case.
        Dictionary<string, object?> properties =
            MessagePropertySerializer.Deserialize(overriddenProperties ?? propertiesJson);

        MessageEnvelope envelope = new()
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            Subject = subject,
            ContentType = contentType,
            Body = body,
            ApplicationProperties = properties,
            SessionId = sessionId,
            ReplyTo = replyTo,
            ReplyToSessionId = replyToSessionId,
            To = to,
            SequenceNumber = sequenceNumber,
            EnqueuedTime = enqueuedTime,
            DeliveryCount = deliveryCount,
            LockToken = lockToken,
            LockedUntil = lockedUntil,
            State = fromDeadLetterQueue ? MessageState.DeadLettered : MessageState.Locked,
            DeadLetterReason = dlqReason,
            DeadLetterDescription = dlqDescription,
        };

        return new ReceivedMessage
        {
            DeliveryId = deliveryId,
            LockToken = lockToken,
            LockedUntil = lockedUntil,
            Message = envelope,
        };
    }

    private static void AddParameter(DbCommand command, string name, DbType type, object? value)
    {
        DbParameter parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
