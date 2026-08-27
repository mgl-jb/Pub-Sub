using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Sample.Orders.Api;

/// <summary>Builds a context for the EF Core command-line tools.</summary>
/// <remarks>Design-time only; generating a migration never touches a database.</remarks>
public sealed class OrdersDbContextFactory : IDesignTimeDbContextFactory<OrdersDbContext>
{
    /// <inheritdoc />
    public OrdersDbContext CreateDbContext(string[] args)
    {
        DbContextOptionsBuilder<OrdersDbContext> builder = new();
        builder.UseSqlServer("Server=(localdb)\\MSSQLLocalDB;Database=SampleOrders;Trusted_Connection=True;");
        return new OrdersDbContext(builder.Options);
    }
}
