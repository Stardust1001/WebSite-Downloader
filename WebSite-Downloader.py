"""
网站下载器
"""
__author__ = 'Stardust1001'

from urllib import request, error
from urllib.request import Request, urlopen, urljoin, urlretrieve, urlparse
import os, shutil, re, time, threading, http
from http import cookiejar
from queue import Queue, Empty
import logging

import socket

socket.setdefaulttimeout(20)

import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


def init_opener():
	cookie = cookiejar.CookieJar()
	cookie_support = request.HTTPCookieProcessor(cookie)
	return request.build_opener(cookie_support)

opener = init_opener()

def init_logger():
	logger = logging.getLogger()
	logger.setLevel(logging.INFO)
	console_handler = logging.StreamHandler()
	console_handler.setLevel(logging.INFO)
	file_handler = logging.FileHandler('log.log', mode='w', encoding='UTF-8')
	file_handler.setLevel(logging.NOTSET)
	formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
	console_handler.setFormatter(formatter)
	file_handler.setFormatter(formatter)
	logger.addHandler(console_handler)
	logger.addHandler(file_handler)
	return logger

logger = init_logger()


class Manager:
	"""
	爬虫主线程的管理器
	从子线程里获取新的链接，处理后添加进要爬取的链接 Queue 队列
	子线程从主线程提供的链接 Queue 队列获取链接进行爬取
	"""
	def __init__(self, home_url):
		# 爬取网站域名的各个子域名

		# 下载的网站的根文件夹，网站可能有不同子域名，提供一个更高级的文件夹路径 -site

		home_dir = '{0}-site/{1}'.format(home_url.split('.')[1], home_url.split('/')[2])
		# home_dir = '/Users/liebeu/Desktop/localhost-site/localhost'

		if os.path.exists(home_dir):
			shutil.rmtree(os.path.dirname(home_dir))
		os.makedirs(home_dir)

		parsed_url = urlparse(home_url)
		scheme = parsed_url.scheme
		# 爬取的网站的顶级域名
		top_domain = '.'.join(parsed_url.netloc.split('.')[1:])
		# 每个请求最大尝试次数
		max_tries = 3

		# 要爬取的链接 Queue 队列
		self.link_queue = Queue()
		self.link_queue.put(home_url)
		# 链接 set ，对新连接进行唯一性判断，然后添加进 Queue 队列
		self.links = set([home_url])
		# 子线程爬虫列表
		self.spiders = []
		# 默认开启 8 个子线程
		for i in range(8):
			self.spiders.append(Spider(home_dir, home_url, self.link_queue, scheme, top_domain, max_tries))

	def start(self):
		"""
		开启主线程的爬虫管理器
		"""
		for spider in self.spiders:
			spider.start()
		# 上次有新链接的时间，默认延时 60 秒，超过时间就结束程序
		last_new_time = time.time()
		# 从子线程获取新链接，添加进 Queue 队列
		while True:
			for spider in self.spiders:
				new_links = spider.get_links()
				if new_links:
					last_new_time = time.time()
				for link in new_links:
					if not link in self.links and len(link) < 250:
						sharp_index = link.find('#')
						if sharp_index > 0:
							link = link[0:sharp_index]
						self.links.add(link)
						self.link_queue.put(link, True)
			if time.time() - last_new_time >= 60:
				break
		# 响铃提醒下载完成
		for i in range(10):
			print('\a')
			time.sleep(0.5)


class Spider(threading.Thread):
	"""
	爬虫线程
	从主线程获取链接进行爬取，并处理 html 、css 文件获取新链接，以及直接下载其他文件
	"""
	def __init__(self, home_dir, home_url, link_queue, scheme, top_domain, max_tries):
		threading.Thread.__init__(self)
		self.home_dir = home_dir
		self.home_url = home_url
		self.link_queue = link_queue
		self.scheme = scheme
		self.top_domain = top_domain
		self.max_tries = max_tries
		# 直接下载的其他文件格式
		self.other_suffixes = set([
			'js', 'jpg', 'png', 'gif', 'svg', 'json', 'xml', 'ico', 'jpeg', 'ttf', 'mp3', 'mp4', 'wav',
			'doc', 'xls', 'pdf', 'docx', 'xlsx', 'eot', 'woff', 'csv', 'swf', 'tar', 'gz', 'zip', 'rar', 'txt',
			'exe', 'ppt', 'pptx', 'm3u8', 'avi', 'wsf'
		])
		self.media_suffixes = set(['mp3', 'mp4', 'pdf', 'gz', 'tar', 'zip', 'rar', 'wav', 'm3u8', 'avi'])
		# 域名名称
		self.domain_names = set(['com', 'cn', 'net', 'org', 'gov', 'io'])
		# html 内容里的链接匹配
		self.html_pat = re.compile(r'(href|src)=(\"|\')([^\"\']*)')
		# css 内容里的链接匹配
		self.css_pat = re.compile(r'url\((\"|\')([^\"\']*)')

		self.links = set()

	def run(self):
		logger.info('{0} start.'.format(threading.current_thread().name))
		# 尝试从主线程的链接队列获取新链接，默认延时 60 秒结束线程
		while True:
			try:
				link = self.link_queue.get(timeout=60)
				self.spide(link)
			except Empty:
				break
		logger.info('{0} end.'.format(threading.current_thread().name))

	def spide(self, link):
		# 爬取链接，对不同链接不同处理
		try:
			suffix = link.split('.')[-1].lower()
			if suffix == 'css':
				self.handle_css(link)
			elif suffix in self.other_suffixes:
				self.download(link)
			else:
				self.handle_html(link)
		except:
			logger.error('[Unknown Error]\t{0}'.format(link))

	def handle_html(self, link):
		# 处理 html 链接
		html = self.get_res(link)
		if html is None:
			return
		html_raw_links = set([ele[2] for ele in self.html_pat.findall(html)])
		html_raw_links = html_raw_links.union([ele[1] for ele in self.css_pat.findall(html)])
		if html_raw_links:
			# 提取有效的链接
			valid_links = list(filter(self.is_valid_link, html_raw_links))
			# 对有效的链接进行处理
			handled_links = list(map(self.handle_valid_link, valid_links))
			# 把有效的链接放入线程的 links ，供主线程爬虫管理器获取
			self.links = self.links.union([urljoin(link, t_link) for t_link in handled_links])
			# 替换 html 内容里的链接为本地网站文件夹里的相对路径
			html = self.replace_links(html, valid_links, self.normalize_link(link))
		# 保存 html 文件
		with open(self.make_filepath(self.normalize_link(link)), 'w') as f_w:
			f_w.write(html)
		logger.info('Handled\t{0}'.format(link))

	def handle_css(self, link):
		"""
		处理 css 链接
		"""
		text = self.get_res(link)
		if text is None:
			return
		css_raw_links = set([ele[1] for ele in self.css_pat.findall(text)])
		if css_raw_links:
			css_raw_links = list(filter(self.is_valid_link, css_raw_links))
			self.links = self.links.union([urljoin(link, t_link) for t_link in css_raw_links])
			text = self.replace_links(text, css_raw_links, self.normalize_link(link))
		with open(self.make_filepath(self.normalize_link(link)), 'w') as f_w:
			f_w.write(text)
		logger.info('Handled\t{0}'.format(link))

	def is_valid_link(self, link):
		"""
		检测有效链接
		嵌入的 data:image 图片不作为新链接
		os.path.relpath 返回值最前面多一个 . 需要删掉
		"""
		if link.find('javascript:') >= 0 or link.find('@') >= 0 or link.find('data:image') >= 0:
			return False
		if link.find('http') >= 0:
			netloc = urlparse(link).netloc
			if netloc:
				if netloc.find(':80') > 0:
					netloc = netloc.replace(':80', '')
				return netloc[netloc.find('.') + 1:] == self.top_domain
		return True

	def handle_valid_link(self, link):
		"""
		处理链接的错误 协议 写法
		http:www.baidu.com http:/www.baidu.com 转换为 http://www.baidu.com
		"""
		if not link:
			return link
		if link[0:2] == '//':
			return self.scheme + link
		if link[0] == '/':
			return urljoin(self.home_url, link)
		if link.find('http') < 0 or link.find('http://') >= 0 or link.find('https://') >= 0:
			return link
		if link.find('http:/') >= 0 or link.find('https:/') >= 0:
			return link.replace(':/', '://')
		if link.find('http:') >= 0 or link.find('https:') >= 0:
			first_colon = link.find(':')
			link = link[0:first_colon] + '://' + link[first_colon + 1:]
			return link
		return link

	def get_res(self, link):
		"""
		获取 html 、 css 链接的响应
		"""
		num_tries = 0
		# 多次尝试获取
		while num_tries < self.max_tries:
			try:
				res = opener.open(Request(link)).read()
				break
			except error.HTTPError:
				logger.error('[error.HTTPError]\t{0}'.format(link))
				return None
			except error.URLError:
				logger.error('[error.URLError]\t{0}'.format(link))
				return None
			except UnicodeEncodeError:
				logger.error('[UnicodeEncodeError]\t{0}'.format(link))
				return None
			except http.client.BadStatusLine:
				logger.error('[http.client.BadStatusLine]\t{0}'.format(link))
				return None
			except http.client.IncompleteRead:
				logger.error('[http.client.IncompleteRead]\t{0}'.format(link))
				return None
			except TimeoutError:
				logger.error('[TimeoutError]\t{0}'.format(link))
				num_tries += 1
			except socket.timeout:
				logger.error('[socket.timeout]\t{0}'.format(link))
				num_tries += 1
			except http.client.RemoteDisconnected:
				logger.error('[RemoteDisconnected]\t{0}'.format(link))
				num_tries += 1
			except ConnectionResetError:
				logger.error('[ConnectionResetError]\t{0}'.format(link))
				num_tries += 1
		if num_tries >= self.max_tries:
			logger.warning('[failed get]\t{0}'.format(link))
			return None
		# 解码响应内容
		try:
			text = res.decode('utf-8')
			return text
		except UnicodeDecodeError:
			pass
		try:
			text = res.decode('gb2312')
			return text
		except UnicodeDecodeError:
			pass
		try:
			text = res.decode('gbk')
			return text
		except UnicodeDecodeError:
			pass
		logger.error('[UnicodeDecodeError]\t{0}'.format(link))
		return None

	def download(self, link):
		"""
		直接下载其他格式的文件
		"""
		socket.setdefaulttimeout(20)
		if link.split('.')[-1].lower() in self.media_suffixes:
			socket.setdefaulttimeout(600)
		num_tries = 0
		# 多次尝试下载
		while num_tries < self.max_tries:
			try:
				urlretrieve(link, self.make_filepath(link))
				break
			except error.HTTPError:
				logger.error('[error.HTTPError]\t{0}'.format(link))
				break
			except error.URLError:
				logger.error('[error.URLError]\t{0}'.format(link))
				break
			except UnicodeEncodeError:
				logger.error('[UnicodeEncodeError]\t{0}'.format(link))
				break
			except http.client.BadStatusLine:
				logger.error('[http.client.BadStatusLine]\t{0}'.format(link))
				break
			except http.client.IncompleteRead:
				logger.error('[http.client.IncompleteRead]\t{0}'.format(link))
				break
			except TimeoutError:
				logger.error('[TimeoutError]\t{0}'.format(link))
				num_tries += 1
			except socket.timeout:
				logger.error('[socket.timeout]\t{0}'.format(link))
				num_tries += 1
			except http.client.RemoteDisconnected:
				logger.error('[RemoteDisconnected]\t{0}'.format(link))
				num_tries += 1
			except ConnectionResetError:
				logger.error('[ConnectionResetError]\t{0}'.format(link))
				num_tries += 1
		if num_tries >= self.max_tries:
			logger.warning('[failed download]\t{0}'.format(link))
		logger.info('Downloaded\t{0}'.format(link))

	def make_filepath(self, link):
		"""
		把链接创建为本地网站文件夹的绝对路径
		"""
		# 需要的话创建新文件夹
		abs_filepath = self.get_abs_filepath(link)
		dirname = os.path.dirname(abs_filepath)
		if not os.path.exists(dirname):
			try:
				os.makedirs(dirname)
			except FileExistsError:
				pass
			except NotADirectoryError:
				logger.error('[NotADirectoryError]\t{0}\t{1}'.format(link, abs_filepath))
		return abs_filepath

	def get_abs_filepath(self, link):
		"""
		把链接转换为本地网站文件夹的绝对路径
		"""
		old_link = link

		if link[-1] == '/':
			link += 'index.html'
		elif link.split('.')[-1] in self.domain_names:
			link += '/index.html'
		rel_url = os.path.relpath(link, self.home_url)
		if rel_url.find('?') >= 0:
			rel_url += '.html'
		if rel_url.split('/')[-1].find('.') < 0 or rel_url == '.':
			rel_url += 'index.html'
		abs_filepath = os.path.join(self.home_dir, rel_url)
		if abs_filepath.find('..') > 0:
			parts = abs_filepath.split('..')
			abs_filepath = '/'.join(parts[0].split('/')[0:-2]) + parts[1]
		if os.path.isdir(abs_filepath):
			logger.warning('[isdir]\t{0}\t{1}'.format(old_link, abs_filepath))
			abs_filepath = os.path.join(abs_filepath, 'index.html')
		return abs_filepath

	def replace_links(self, content, links, cur_url):
		"""
		替换 html 、 css 内容里的链接
		"""
		links.sort(key=lambda link: len(link), reverse=True)
		for link in set(links):
			link_abspath = self.get_abs_filepath(urljoin(cur_url, self.normalize_link(link)))
			cur_url_abspath = self.get_abs_filepath(cur_url)
			rel_link = os.path.relpath(link_abspath, cur_url_abspath)[1:].replace('?', '%3F')
			replacement = '"{0}"'.format(rel_link)
			content = content.replace(
				'"{0}"'.format(link),replacement
				).replace('\'{0}\''.format(link), replacement)
		return content

	def normalize_link(self, link):
		if link.find('http') < 0:
			return link
		if link.find(':80') > 0:
			link = link.replace(':80', '')
		first_colon = link.find(':')
		link = self.scheme + link[first_colon:]
		return link

	def get_links(self):
		"""
		主线程爬虫管理器从这里获取爬虫子线程的新链接
		获取后子线程就删除旧链接，为后面获取的链接做准备
		"""
		export_links = self.links.copy()
		self.links.clear()
		return export_links


if __name__ == '__main__':
	manager = Manager('http://www.whsw.net/')
	manager.start()
