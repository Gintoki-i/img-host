"""
GitHub Image Host (GitHub 图床工具)

功能说明：
- 将图片 / 文件上传到 GitHub 仓库指定目录
- 通过 jsDelivr 或自定义 CDN 访问
- 支持：
    - 单个上传 / 删除 / 更新 / 查询
    - 批量上传 / 删除 / 更新
    - 自动创建目录（.gitkeep）
    - 清空目录但保留指定文件

典型使用场景：
- 个人 / 项目图床
- 文档图片托管
- 飞书 / Notion / Markdown 外链图片
"""

import os
import base64
import json
import threading
import time
from pathlib import Path
from typing import Iterable, Dict, Optional, Set

import requests


class GitHubImageHost:
    """
    GitHub 图床客户端

    一个实例 = 一个 GitHub 仓库 + 一个目录
    """

    def __init__(
        self,
        *,
        owner: str,
        repo: str,
        branch: str = "main",
        subdir: str = "img/",
        token: Optional[str] = None,
        custom_cdn: Optional[str] = None,
        max_concurrency: int = 4,
        session: Optional[requests.Session] = None,
    ):
        """
        初始化 GitHub 图床实例

        Args:
            owner: GitHub 用户名 / 组织名
            repo: 仓库名
            branch: 分支名（默认 main）
            subdir: 仓库内目录（如 img/）
            token: GitHub PAT（不传则读取环境变量 GITHUB_PAT）
            custom_cdn: 自定义 CDN 前缀（不传则使用 jsDelivr）
            max_concurrency: 最大并发上传/删除数
            session: 可复用的 requests.Session
        """
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self.subdir = subdir.strip("/") + "/" if subdir else ""
        self.token = token or os.getenv("GITHUB_PAT")
        self.custom_cdn = custom_cdn

        if not self.token:
            raise RuntimeError("缺少 GitHub PAT，请设置 token 或环境变量 GITHUB_PAT")

        self.session = session or requests.Session()
        self._sema = threading.Semaphore(max_concurrency)

        self._headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    # ======================================================
    # 内部工具方法（不建议外部直接调用）
    # ======================================================
    def _api_url(self, path: str) -> str:
        """
        构造 GitHub contents API URL
        """
        return f"https://api.github.com/repos/{self.owner}/{self.repo}/contents/{path}"

    def _cdn_url(self, filename: str) -> str:
        """
        构造文件的 CDN 访问 URL
        """
        rel = f"{self.subdir}{filename}".lstrip("/")
        if self.custom_cdn:
            return f"{self.custom_cdn.rstrip('/')}/{rel}"
        return f"https://cdn.jsdelivr.net/gh/{self.owner}/{self.repo}@{self.branch}/{rel}"

    def _ensure_dir(self):
        """
        确保 subdir 在仓库中存在

        GitHub 没有“创建目录”的概念，
        实际做法是创建一个 .gitkeep 文件
        """
        if not self.subdir:
            return

        r = self.session.get(self._api_url(self.subdir), headers=self._headers)
        if r.ok:
            return

        body = {
            "message": "init dir",
            "content": base64.b64encode(b"").decode(),
            "branch": self.branch,
        }

        self.session.put(
            self._api_url(f"{self.subdir}.gitkeep"),
            headers=self._headers,
            data=json.dumps(body),
        )

    # ======================================================
    # 单文件 CRUD
    # ======================================================
    def upload(self, file_path: str) -> str:
        """
        上传单个文件到图床

        Args:
            file_path: 本地文件路径

        Returns:
            CDN 访问 URL
        """
        self._ensure_dir()

        p = Path(file_path)
        if not p.is_file():
            raise FileNotFoundError(file_path)

        filename = p.name
        content_b64 = base64.b64encode(p.read_bytes()).decode("ascii")
        api_path = f"{self.subdir}{filename}"

        body = {
            "message": f"upload {filename}",
            "content": content_b64,
            "branch": self.branch,
        }

        with self._sema:
            resp = self.session.put(
                self._api_url(api_path),
                headers=self._headers,
                data=json.dumps(body),
                timeout=30,
            )

            # 文件已存在：自动重命名
            if resp.status_code == 422:
                ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
                filename = f"{p.stem}-{ts}{p.suffix}"
                api_path = f"{self.subdir}{filename}"
                body["message"] = f"upload {filename}"

                resp = self.session.put(
                    self._api_url(api_path),
                    headers=self._headers,
                    data=json.dumps(body),
                    timeout=30,
                )

            if not resp.ok:
                raise RuntimeError(resp.text)

        return self._cdn_url(filename)

    def delete(self, filename: str) -> None:
        """
        删除仓库中的文件（若不存在则忽略）

        Args:
            filename: 仓库中的文件名（不要带目录）
        """
        api_path = f"{self.subdir}{filename}"

        r = self.session.get(self._api_url(api_path), headers=self._headers)
        if r.status_code == 404:
            return

        sha = r.json()["sha"]
        body = {
            "message": f"delete {filename}",
            "sha": sha,
            "branch": self.branch,
        }

        with self._sema:
            resp = self.session.delete(
                self._api_url(api_path),
                headers=self._headers,
                data=json.dumps(body),
            )

        if not resp.ok:
            raise RuntimeError(resp.text)

    def update(self, file_path: str, filename: Optional[str] = None) -> str:
        """
        更新仓库中的文件（覆盖）

        Args:
            file_path: 本地新文件路径
            filename: 仓库中的文件名（默认使用本地文件名）

        Returns:
            CDN URL
        """
        self._ensure_dir()

        p = Path(file_path)
        name = filename or p.name
        api_path = f"{self.subdir}{name}"

        r = self.session.get(self._api_url(api_path), headers=self._headers)
        if not r.ok:
            raise FileNotFoundError(name)

        sha = r.json()["sha"]
        content_b64 = base64.b64encode(p.read_bytes()).decode("ascii")

        body = {
            "message": f"update {name}",
            "content": content_b64,
            "sha": sha,
            "branch": self.branch,
        }

        with self._sema:
            resp = self.session.put(
                self._api_url(api_path),
                headers=self._headers,
                data=json.dumps(body),
            )

        if not resp.ok:
            raise RuntimeError(resp.text)

        return self._cdn_url(name)

    def exists(self, filename: str) -> bool:
        """
        判断文件是否存在于仓库
        """
        r = self.session.get(
            self._api_url(f"{self.subdir}{filename}"),
            headers=self._headers,
        )
        return r.ok

    def get_url(self, filename: str) -> str:
        """
        获取文件的 CDN URL（不检查是否存在）
        """
        return self._cdn_url(filename)

    # ======================================================
    # 目录操作
    # ======================================================
    def clear_dir(
        self,
        *,
        keep: Optional[Set[str]] = None,
        dir_path: Optional[str] = None,
    ) -> int:
        """
        清空目录，但保留指定文件

        Args:
            keep: 需要保留的文件名集合（如 {'.gitkeep'}）
            dir_path: 指定目录（默认当前 subdir）

        Returns:
            删除的文件数量
        """
        target_dir = dir_path.strip("/") + "/" if dir_path else self.subdir
        keep = keep or set()

        r = self.session.get(self._api_url(target_dir), headers=self._headers)
        if not r.ok:
            raise RuntimeError(f"无法读取目录: {target_dir}")

        deleted = 0
        for item in r.json():
            if item["type"] != "file":
                continue
            if item["name"] in keep:
                continue

            body = {
                "message": f"delete {item['name']}",
                "sha": item["sha"],
                "branch": self.branch,
            }

            with self._sema:
                resp = self.session.delete(
                    self._api_url(item["path"]),
                    headers=self._headers,
                    data=json.dumps(body),
                )

            if not resp.ok:
                raise RuntimeError(resp.text)

            deleted += 1

        return deleted

    # ======================================================
    # 批量操作
    # ======================================================
    def upload_many(self, files: Iterable[str]) -> Dict[str, str]:
        """
        批量上传文件

        Returns:
            {本地路径: CDN URL}
        """
        return {f: self.upload(f) for f in files}

    def delete_many(self, filenames: Iterable[str]) -> None:
        """
        批量删除仓库文件
        """
        for f in filenames:
            self.delete(f)

    def update_many(self, files: Dict[str, str]) -> Dict[str, str]:
        """
        批量更新文件

        Args:
            files: {本地路径: 仓库文件名}
        """
        return {src: self.update(src, dst) for src, dst in files.items()}



