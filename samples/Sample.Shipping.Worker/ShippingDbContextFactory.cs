using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Sample.Shipping.Worker;

/// <summary>Builds a context for the EF Core command-line tools.</summary>
/// <remarks>Design-time only; generating a migration never touches a database.</remarks>
public sealed class ShippingDbContextFactory : IDesignTimeDbContextFactory<ShippingDbContext>
{
    /// <inheritdoc />
    public ShippingDbContext CreateDbContext(string[] args)
    {
        DbContextOptionsBuilder<ShippingDbContext> builder = new();
        builder.UseSqlServer("Server=(localdb)\\MSSQLLocalDB;Database=SampleShipping;Trusted_Connection=True;");
        return new ShippingDbContext(builder.Options);
    }
}
