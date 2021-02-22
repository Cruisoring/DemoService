using System;
using System.Collections.Generic;
using System.Text;

namespace DemoService.Core.Models
{
    public interface IDelta
    {
        bool WithDifference();
    }
}
