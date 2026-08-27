using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace PubSub.Broker.Core.Storage.Migrations
{
    /// <inheritdoc />
    public partial class InitialBrokerSchema : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "DedupEntries",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    TopicId = table.Column<int>(type: "int", nullable: false),
                    MessageId = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: false),
                    SequenceNumber = table.Column<long>(type: "bigint", nullable: false),
                    PublishedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    ExpiresAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_DedupEntries", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Topics",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Name = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: false),
                    DefaultTimeToLive = table.Column<long>(type: "bigint", nullable: false),
                    DuplicateDetectionEnabled = table.Column<bool>(type: "bit", nullable: false),
                    DuplicateDetectionWindow = table.Column<long>(type: "bigint", nullable: false),
                    MaxMessageSizeBytes = table.Column<int>(type: "int", nullable: false),
                    PublishingSuspended = table.Column<bool>(type: "bit", nullable: false),
                    CreatedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Topics", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Messages",
                columns: table => new
                {
                    SequenceNumber = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    TopicId = table.Column<int>(type: "int", nullable: false),
                    MessageId = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: false),
                    CorrelationId = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: true),
                    Subject = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: true),
                    ContentType = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: false),
                    Body = table.Column<byte[]>(type: "varbinary(max)", nullable: false),
                    ApplicationPropertiesJson = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    SessionId = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: true),
                    ReplyTo = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: true),
                    ReplyToSessionId = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: true),
                    To = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: true),
                    EnqueuedTime = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    ExpiresAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Messages", x => x.SequenceNumber);
                    table.ForeignKey(
                        name: "FK_Messages_Topics_TopicId",
                        column: x => x.TopicId,
                        principalTable: "Topics",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Subscriptions",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    TopicId = table.Column<int>(type: "int", nullable: false),
                    Name = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: false),
                    LockDuration = table.Column<long>(type: "bigint", nullable: false),
                    MaxDeliveryCount = table.Column<int>(type: "int", nullable: false),
                    RequiresSession = table.Column<bool>(type: "bit", nullable: false),
                    SessionLockDuration = table.Column<long>(type: "bigint", nullable: false),
                    DeadLetterOnMessageExpiration = table.Column<bool>(type: "bit", nullable: false),
                    DeadLetterOnFilterEvaluationError = table.Column<bool>(type: "bit", nullable: false),
                    DefaultTimeToLive = table.Column<long>(type: "bigint", nullable: true),
                    ReceivingSuspended = table.Column<bool>(type: "bit", nullable: false),
                    CreatedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    RulesVersion = table.Column<int>(type: "int", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Subscriptions", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Subscriptions_Topics_TopicId",
                        column: x => x.TopicId,
                        principalTable: "Topics",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Deliveries",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    MessageSequenceNumber = table.Column<long>(type: "bigint", nullable: false),
                    SubscriptionId = table.Column<int>(type: "int", nullable: false),
                    SequenceNumber = table.Column<long>(type: "bigint", nullable: false),
                    SessionId = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: true),
                    State = table.Column<int>(type: "int", nullable: false),
                    AvailableAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    DeliveryCount = table.Column<int>(type: "int", nullable: false),
                    LockToken = table.Column<Guid>(type: "uniqueidentifier", nullable: true),
                    LockedUntil = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: true),
                    LockedBy = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: true),
                    DeadLetterReason = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: true),
                    DeadLetterDescription = table.Column<string>(type: "nvarchar(2048)", maxLength: 2048, nullable: true),
                    OverriddenPropertiesJson = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    SettledAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: true),
                    ExpiresAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    CreatedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Deliveries", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Deliveries_Messages_MessageSequenceNumber",
                        column: x => x.MessageSequenceNumber,
                        principalTable: "Messages",
                        principalColumn: "SequenceNumber",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Deliveries_Subscriptions_SubscriptionId",
                        column: x => x.SubscriptionId,
                        principalTable: "Subscriptions",
                        principalColumn: "Id");
                });

            migrationBuilder.CreateTable(
                name: "Rules",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    SubscriptionId = table.Column<int>(type: "int", nullable: false),
                    Name = table.Column<string>(type: "nvarchar(260)", maxLength: 260, nullable: false),
                    FilterKind = table.Column<int>(type: "int", nullable: false),
                    SqlExpression = table.Column<string>(type: "nvarchar(max)", maxLength: 4096, nullable: true),
                    CorrelationJson = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    ActionExpression = table.Column<string>(type: "nvarchar(max)", maxLength: 4096, nullable: true),
                    CreatedAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Rules", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Rules_Subscriptions_SubscriptionId",
                        column: x => x.SubscriptionId,
                        principalTable: "Subscriptions",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "SessionLocks",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    SubscriptionId = table.Column<int>(type: "int", nullable: false),
                    SessionId = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: false),
                    LockToken = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    LockedUntil = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false),
                    LockedBy = table.Column<string>(type: "nvarchar(128)", maxLength: 128, nullable: true),
                    State = table.Column<byte[]>(type: "varbinary(max)", nullable: true),
                    AcquiredAt = table.Column<DateTimeOffset>(type: "datetimeoffset", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SessionLocks", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SessionLocks_Subscriptions_SubscriptionId",
                        column: x => x.SubscriptionId,
                        principalTable: "Subscriptions",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_DedupEntries_Expiry",
                table: "DedupEntries",
                column: "ExpiresAt");

            migrationBuilder.CreateIndex(
                name: "UX_DedupEntries_MessageId",
                table: "DedupEntries",
                columns: new[] { "TopicId", "MessageId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Deliveries_Claim",
                table: "Deliveries",
                columns: new[] { "SubscriptionId", "State", "AvailableAt", "SequenceNumber" });

            migrationBuilder.CreateIndex(
                name: "IX_Deliveries_Expiry",
                table: "Deliveries",
                columns: new[] { "State", "ExpiresAt" });

            migrationBuilder.CreateIndex(
                name: "IX_Deliveries_LockExpiry",
                table: "Deliveries",
                columns: new[] { "State", "LockedUntil" });

            migrationBuilder.CreateIndex(
                name: "IX_Deliveries_MessageSequenceNumber",
                table: "Deliveries",
                column: "MessageSequenceNumber");

            migrationBuilder.CreateIndex(
                name: "IX_Deliveries_Sequence",
                table: "Deliveries",
                columns: new[] { "SubscriptionId", "SequenceNumber" });

            migrationBuilder.CreateIndex(
                name: "IX_Deliveries_SessionClaim",
                table: "Deliveries",
                columns: new[] { "SubscriptionId", "SessionId", "State", "SequenceNumber" });

            migrationBuilder.CreateIndex(
                name: "IX_Deliveries_Settled",
                table: "Deliveries",
                columns: new[] { "SubscriptionId", "State", "SettledAt" });

            migrationBuilder.CreateIndex(
                name: "IX_Messages_TopicId_ExpiresAt",
                table: "Messages",
                columns: new[] { "TopicId", "ExpiresAt" });

            migrationBuilder.CreateIndex(
                name: "IX_Rules_SubscriptionId_Name",
                table: "Rules",
                columns: new[] { "SubscriptionId", "Name" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_SessionLocks_Expiry",
                table: "SessionLocks",
                column: "LockedUntil");

            migrationBuilder.CreateIndex(
                name: "UX_SessionLocks_Session",
                table: "SessionLocks",
                columns: new[] { "SubscriptionId", "SessionId" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Subscriptions_TopicId_Name",
                table: "Subscriptions",
                columns: new[] { "TopicId", "Name" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Topics_Name",
                table: "Topics",
                column: "Name",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "DedupEntries");

            migrationBuilder.DropTable(
                name: "Deliveries");

            migrationBuilder.DropTable(
                name: "Rules");

            migrationBuilder.DropTable(
                name: "SessionLocks");

            migrationBuilder.DropTable(
                name: "Messages");

            migrationBuilder.DropTable(
                name: "Subscriptions");

            migrationBuilder.DropTable(
                name: "Topics");
        }
    }
}
