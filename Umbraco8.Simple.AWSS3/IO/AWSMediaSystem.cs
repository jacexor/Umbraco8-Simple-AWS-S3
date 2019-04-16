using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using Umbraco.Core.IO;
using Umbraco8.Simple.AWSS3.Extensions;

namespace Umbraco8.Simple.AWSS3.IO
{
	public class AWSMediaSystem : IFileSystem
	{
		protected readonly string _bucketName;
		protected readonly string _awsKey;
		protected readonly string _awsSecret;
		protected readonly string _awsBucketHostname;
		protected readonly string _awsBucketPrefix;
		protected readonly string _awsRegion;
		protected const string _delimiter = "/";
		protected const int _batchSize = 1000;

		public Func<IAmazonS3> ClientFactory { get; set; }

		public bool CanAddPhysical => throw new NotImplementedException();


		public AWSMediaSystem()
		{
			_bucketName = ConfigurationManager.AppSettings["awsBucketName"];
			_awsBucketHostname = ConfigurationManager.AppSettings["awsBucketHostname"];
			_awsBucketPrefix = ConfigurationManager.AppSettings["awsBucketPrefix"];
			_awsKey = ConfigurationManager.AppSettings["awsKey"];
			_awsSecret = ConfigurationManager.AppSettings["awsSecret"];
			_awsRegion = ConfigurationManager.AppSettings["awsRegion"];

			var regionEndpoint = RegionEndpoint.GetBySystemName(_awsRegion);
			ClientFactory = () => new AmazonS3Client(_awsKey, _awsSecret, regionEndpoint);
		}

		protected virtual T Execute<T>(Func<IAmazonS3, T> request)
		{
			using (var client = ClientFactory())
			{
				try
				{
					return request(client);
				}
				catch (AmazonS3Exception ex)
				{
					if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
						throw new FileNotFoundException(ex.Message, ex);
					if (ex.StatusCode == System.Net.HttpStatusCode.Unauthorized)
						throw new UnauthorizedAccessException(ex.Message, ex);
					throw;
				}
			}
		}

		protected virtual IEnumerable<ListObjectsResponse> ExecuteWithContinuation(ListObjectsRequest request)
		{
			var response = Execute(client => client.ListObjects(request));
			yield return response;

			while (response.IsTruncated)
			{
				request.Marker = response.NextMarker;
				response = Execute(client => client.ListObjects(request));
				yield return response;
			}
		}

		protected virtual string ResolveBucketPath(string path, bool isDir = false)
		{
			if (string.IsNullOrEmpty(path))
				return _awsBucketPrefix;

			//Remove Bucket Hostname
			if (!path.Equals("/") && path.StartsWith(_awsBucketHostname, StringComparison.InvariantCultureIgnoreCase))
				path = path.Substring(_awsBucketHostname.Length);

			path = path.Replace("\\", _delimiter);
			if (path == _delimiter)
				return _awsBucketPrefix;

			if (path.StartsWith(_delimiter))
				path = path.Substring(1);

			//Remove Key Prefix If Duplicate
			if (path.StartsWith(_awsBucketPrefix, StringComparison.InvariantCultureIgnoreCase))
				path = path.Substring(_awsBucketPrefix.Length);

			if (isDir && (!path.EndsWith(_delimiter)))
				path = string.Concat(path, _delimiter);

			return string.Concat(_awsBucketPrefix, "/" + path);
		}

		protected virtual string RemovePrefix(string key)
		{
			if (!string.IsNullOrEmpty(_awsBucketPrefix) && key.StartsWith(_awsBucketPrefix))
				key = key.Substring(_awsBucketPrefix.Length);

			if (key.EndsWith(_delimiter))
				key = key.Substring(0, key.Length - _delimiter.Length);
			return key;
		}


		public void AddFile(string path, Stream stream)
		{
			using (var memoryStream = new MemoryStream())
			{
				stream.CopyTo(memoryStream);
				var request = new PutObjectRequest
				{
					BucketName = _bucketName,
					Key = ResolveBucketPath(path),
					CannedACL = S3CannedACL.PublicRead,
					ContentType = System.Web.MimeMapping.GetMimeMapping(path),
					InputStream = memoryStream,
					ServerSideEncryptionMethod = ServerSideEncryptionMethod.None
				};

				var response = Execute(client => client.PutObject(request));
			}
		}

		public void AddFile(string path, Stream stream, bool overrideIfExists)
		{
			using (var memoryStream = new MemoryStream())
			{
				stream.CopyTo(memoryStream);
				var request = new PutObjectRequest
				{
					BucketName = _bucketName,
					Key = ResolveBucketPath(path),
					CannedACL = S3CannedACL.PublicRead,
					ContentType = System.Web.MimeMapping.GetMimeMapping(path),
					InputStream = memoryStream,
					ServerSideEncryptionMethod = ServerSideEncryptionMethod.None
				};

				var response = Execute(client => client.PutObject(request));
			}
		}

		public IEnumerable<string> GetDirectories(string path)
		{
			if (string.IsNullOrEmpty(path))
				path = "/";

			path = ResolveBucketPath(path, true);
			var request = new ListObjectsRequest
			{
				BucketName = _bucketName,
				Delimiter = _delimiter,
				Prefix = path
			};

			var response = ExecuteWithContinuation(request);
			return response
				.SelectMany(p => p.CommonPrefixes)
				.Select(p => RemovePrefix(p))
				.ToArray();
		}

		public void DeleteDirectory(string path)
		{
			DeleteDirectory(path, false);
		}

		public void DeleteDirectory(string path, bool recursive)
		{
			//List Objects To Delete
			var listRequest = new ListObjectsRequest
			{
				BucketName = _bucketName,
				Prefix = ResolveBucketPath(path, true)
			};

			var listResponse = ExecuteWithContinuation(listRequest);
			var keys = listResponse
				.SelectMany(p => p.S3Objects)
				.Select(p => new KeyVersion { Key = p.Key })
				.ToArray();

			//Batch Deletion Requests
			foreach (var items in keys.Batch(_batchSize))
			{
				var deleteRequest = new DeleteObjectsRequest
				{
					BucketName = _bucketName,
					Objects = items.ToList()
				};
				Execute(client => client.DeleteObjects(deleteRequest));
			}
		}

		public bool DirectoryExists(string path)
		{
			var request = new ListObjectsRequest
			{
				BucketName = _bucketName,
				Prefix = ResolveBucketPath(path, true),
				MaxKeys = 1
			};

			var response = Execute(client => client.ListObjects(request));
			return response.S3Objects.Count > 0;
		}

		public IEnumerable<string> GetFiles(string path)
		{
			return GetFiles(path, "*.*");
		}

		public IEnumerable<string> GetFiles(string path, string filter)
		{
			path = ResolveBucketPath(path, true);

			string filename = Path.GetFileNameWithoutExtension(filter);
			if (filename.EndsWith("*"))
				filename = filename.Remove(filename.Length - 1);

			string ext = Path.GetExtension(filter);
			if (ext.Contains("*"))
				ext = string.Empty;

			var request = new ListObjectsRequest
			{
				BucketName = _bucketName,
				Delimiter = _delimiter,
				Prefix = path + filename
			};

			var response = ExecuteWithContinuation(request);
			return response
				.SelectMany(p => p.S3Objects)
				.Select(p => RemovePrefix(p.Key))
				.Where(p => !string.IsNullOrEmpty(p) && p.EndsWith(ext))
				.ToArray();
		}

		public Stream OpenFile(string path)
		{
			var request = new GetObjectRequest
			{
				BucketName = _bucketName,
				Key = ResolveBucketPath(path)
			};

			MemoryStream stream;
			using (var response = Execute(client => client.GetObject(request)))
			{
				stream = new MemoryStream();
				response.ResponseStream.CopyTo(stream);
			}
			stream.Seek(0, SeekOrigin.Begin);
			return stream;
		}

		public void DeleteFile(string path)
		{
			var request = new DeleteObjectRequest
			{
				BucketName = _bucketName,
				Key = ResolveBucketPath(path)
			};
			Execute(client => client.DeleteObject(request));
		}

		public bool FileExists(string path)
		{
			var request = new GetObjectMetadataRequest
			{
				BucketName = _bucketName,
				Key = ResolveBucketPath(path)
			};

			try
			{
				Execute(client => client.GetObjectMetadata(request));
				return true;
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		public string GetRelativePath(string fullPathOrUrl)
		{
			if (string.IsNullOrEmpty(fullPathOrUrl))
				return string.Empty;

			if (fullPathOrUrl.StartsWith(_delimiter))
				fullPathOrUrl = fullPathOrUrl.Substring(1);

			//Strip Hostname
			if (fullPathOrUrl.StartsWith(_awsBucketHostname, StringComparison.InvariantCultureIgnoreCase))
				fullPathOrUrl = fullPathOrUrl.Substring(_awsBucketHostname.Length);

			//Strip Bucket Prefix
			if (fullPathOrUrl.StartsWith(_awsBucketPrefix, StringComparison.InvariantCultureIgnoreCase))
				return fullPathOrUrl.Substring(_awsBucketPrefix.Length);

			return fullPathOrUrl;
		}

		public string GetFullPath(string path)
		{
			return path;
		}

		public string GetUrl(string path)
		{
			return string.Concat(_awsBucketHostname, ResolveBucketPath(path));
		}

		public DateTimeOffset GetLastModified(string path)
		{
			var request = new GetObjectMetadataRequest
			{
				BucketName = _bucketName,
				Key = ResolveBucketPath(path)
			};

			try
			{
				var response = Execute(client => client.GetObjectMetadata(request));
				return new DateTimeOffset(response.LastModified);
			}
			catch (FileNotFoundException)
			{
				return DateTimeOffset.MinValue;
			}
		}

		public DateTimeOffset GetCreated(string path)
		{
			//It Is Not Possible To Get Object Created Date - Bucket Versioning Required
			//Return Last Modified Date Instead
			return GetLastModified(path);
		}






		#region Not-Implemented

		public long GetSize(string path)
		{
			throw new NotImplementedException();
		}


		public void AddFile(string path, string physicalPath, bool overrideIfExists = true, bool copy = false)
		{
			throw new NotImplementedException();
		}

		#endregion
	}
}
