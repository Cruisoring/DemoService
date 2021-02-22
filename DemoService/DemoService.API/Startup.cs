using System;
using System.Collections.Generic;
using System.IO;
using DemoService.Core.Repositories;
using DemoService.Core.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Swashbuckle.AspNetCore.Swagger;
using System.Reflection;
using Newtonsoft.Json.Linq;
using ServiceStack.DataAnnotations;


namespace DemoService.API
{
    public class Startup
    {
        protected readonly AssemblyName assemblyName = typeof(Startup).Assembly.GetName();

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddScoped<IDemoService, Core.Services.DemoService>();
            services.AddScoped<IDemoRepository, DemoRepository>();

            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);

            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v" + assemblyName.Version, new Info
                {
                    Title = assemblyName.Name,
                    Version = $"v{assemblyName.Version}"
                });
            });
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app">App to be launched.</param>
        /// <param name="env">Environment.</param>
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment()) app.UseDeveloperExceptionPage();

            app.UseMvc();

            app.UseSwagger();
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint($"/swagger/v{assemblyName.Version}/swagger.json", assemblyName.Name);
            });
        }
    }
}