using System.Threading.Tasks;

namespace DemoService.Core.Repositories
{
    public interface IDemoRepository
    {
        Task<int> DemoMethodAsync(int id);
    }

    public class DemoRepository : IDemoRepository
    {
        public Task<int> DemoMethodAsync(int id)
        {
            return Task.FromResult(id);
        }
    }
}
