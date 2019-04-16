using Umbraco.Core.Composing;
using Umbraco.Core.IO;
using Umbraco8.Simple.AWSS3.IO;

namespace Umbraco8.Simple.AWSS3
{
	public class Startup : ComponentComposer<AWSS3Component>
	{
		public override void Compose(Composition composition)
		{
			composition.RegisterUniqueFor<IFileSystem, IMediaFileSystem>(new AWSMediaSystem());
		}
	}

	public class AWSS3Component : IComponent
	{
		public void Initialize() { }

		public void Terminate() { }
	}
}
