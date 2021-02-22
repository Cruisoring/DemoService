using DemoService.Core.Repositories;
using System.Threading.Tasks;

namespace DemoService.Core.Services
{
    public interface IDemoService
    {
        Task<int> DemoMethodAsync(int id);
    }

    public class DemoService : IDemoService
    {
        private readonly IDemoRepository _demoRepository;
        public DemoService(IDemoRepository demoRepository)
        {
            _demoRepository = demoRepository;
        }

        public Task<int> DemoMethodAsync(int id)
        {
            return _demoRepository.DemoMethodAsync(id);
        }
    }
}
