using System;
using System.Collections.Generic;
using ElasticTools.Code.Settings;
using log4net;
using log4net.Config;

namespace ElasticTools
{
	internal class Program
	{
		private static bool _showHelp;
		private static readonly ILog Logger = LogManager.GetLogger(typeof(Program).Name);

		static void Main(string[] args)
		{
			try
			{
				GlobalContext.Properties["LogFilePath"] = AppSettings.LogFilePath;
				XmlConfigurator.Configure();

				var argsOk = ReadArgs(args);

				if (!_showHelp && argsOk)
				{
					Engine.Run();
				}
				else
				{
					ShowHelp();
				}
			}
			catch (Exception ex)
			{
				Logger.ErrorFormat("Exception: {0}", ex.Message);
			}

			Logger.Info("DONE!!!");
		}

		/// <summary>
		///   Reads the arguments.
		/// </summary>
		/// <param name="args">The arguments.</param>
		/// <returns></returns>
		/// <exception cref="System.ArgumentOutOfRangeException">args;Invalid parameter found in the command line.</exception>
		/// <exception cref="System.ArgumentException">Atleast one of ElasticSearch or SQL must be enabled</exception>
		private static bool ReadArgs(IList<string> args)
		{
			try
			{
				if (args.Count == 0)
					return false;

				for (var index = 0; index < args.Count; index++)
				{
					switch (args[index])
					{
						case Arguments.ReIndex:
						case Arguments.ReIndexShort:
							{
								AppSettings.Option = Usage.ReIndex;
								break;
							}

						case Arguments.SourceIndex:
						case Arguments.SourceIndexShort:
							{
								index++;
								AppSettings.SourceIndex = args[index];
								break;
							}

						case Arguments.DestinationIndex:
						case Arguments.DestinationIndexShort:
							{
								index++;
								AppSettings.DestinationIndex = args[index];
								break;
							}

						case Arguments.Type:
						case Arguments.TypeShort:
							{
								index++;
								AppSettings.Type = args[index];
								break;
							}

						case Arguments.Server:
						case Arguments.ServerShort:
							{
								index++;
								AppSettings.Server = args[index];
								break;
							}

						case Arguments.Help:
						case Arguments.HelpShort:
							{
								_showHelp = true;
								break;
							}

						case "":
						case "\n":
							{
								break;
							}
						default:
							{
								throw new ArgumentOutOfRangeException("args", args[index], "Invalid parameter found in the command line.");
							}
					}
				}
			}
			catch (Exception ex)
			{
				Logger.Error(ex.Message);
				return false;
			}
			return true;
		}

		/// <summary>
		///   Shows the help.
		/// </summary>
		private static void ShowHelp()
		{
			Logger.Info(" ");
			Logger.Info("Usage for the ElasticTools:");
			Logger.Info("ESUtility.exe {-E Export | -I Index | -TF Transform | -ETL Transform & Index | -L Lookup}");
			Logger.Info(" ");
			Logger.Info("[ -S Server]\t[-D Database]\t[-U Username]");
			Logger.Info("[ -P Password]\t[-T TrustedConnection]\t[-Q QueryFilePath]");
			Logger.Info("[ -SD StartDate][-ED EndDate]\t[-DI DateInterval(Day)]");
			Logger.Info("[ -G Geo] [-LIX LookupIndex] [-IX Index] [-IXP IndexPattern] [-IXP IndexAlias] [-IP InputPath]");
			Logger.Info("[ -IIN IncludeIndexName] [-Prop Property]");
			Logger.Info("[ -B batchSize] [-TC ThreadCount] [-A Action {Index,Update,Delete}]");
			Logger.Info(" ");
		}
	}
}
