/**
 * C++ base classes to assist implementing the SQLite VFS API:
 *     https://www.sqlite.org/vfs.html
 */
#pragma once

#include <assert.h>
#include <memory>
#include <sqlite3.h>
#include <string.h>
#include <string>

namespace SQLiteVFS {

/**
 * Abstract base class for sqlite3_file implementations. VFS xOpen() implementations can
 * instantiate a subclass implementing all of the pure methods, then call InitHandle() to fill out
 * a sqlite3_file* Handle to hand back to the caller. The instance self-deletes upon xClose().
 */
class File {
  protected:
    sqlite3_io_methods methods_;

    virtual int Close() {
        delete this;
        return SQLITE_OK;
    }
    virtual int Read(void *zBuf, int iAmt, sqlite3_int64 iOfst) = 0;
    virtual int Write(const void *zBuf, int iAmt, sqlite3_int64 iOfst) = 0;
    virtual int Truncate(sqlite3_int64 size) = 0;
    virtual int Sync(int flags) = 0;
    virtual int FileSize(sqlite3_int64 *pSize) = 0;
    virtual int Lock(int eLock) = 0;
    virtual int Unlock(int eLock) = 0;
    virtual int CheckReservedLock(int *pResOut) = 0;
    virtual int FileControl(int op, void *pArg) = 0;
    virtual int SectorSize() = 0;
    virtual int DeviceCharacteristics() = 0;

    virtual int ShmMap(int iPg, int pgsz, int isWrite, void volatile **pp) = 0;
    virtual int ShmLock(int offset, int n, int flags) = 0;
    virtual void ShmBarrier() = 0;
    virtual int ShmUnmap(int deleteFlag) = 0;

    virtual int Fetch(sqlite3_int64 iOfst, int iAmt, void **pp) = 0;
    virtual int Unfetch(sqlite3_int64 iOfst, void *p) = 0;

  public:
    /**
     * sqlite3_file "handle" which VFS xOpen() is responsible for filling in by calling
     * InitHandle()
     */
    struct Handle {
        sqlite3_file base;
        File *this_;
    };

    File() {
        memset(&methods_, 0, sizeof(methods_));
        methods_.iVersion = 3; // subclasses may override to 2 or 1
    }

    virtual ~File() noexcept = default;

    /**
     * Initialize the Handle, assuming pFile points to a Handle.
     */
    virtual void InitHandle(sqlite3_file *pFile) noexcept {
        Handle *handle = (Handle *)pFile;
        assert(methods_.iVersion && !methods_.xClose);
#define __SQLITEVFS_FILE_DISPATCH(Method, pFile, ...)                                              \
    try {                                                                                          \
        return reinterpret_cast<Handle *>(pFile)->this_->Method(__VA_ARGS__);                      \
    } catch (std::bad_alloc &) {                                                                   \
        return SQLITE_NOMEM;                                                                       \
    } catch (...) {                                                                                \
        return SQLITE_ERROR;                                                                       \
    }
        methods_.xClose = [](sqlite3_file *pFile) noexcept {
            __SQLITEVFS_FILE_DISPATCH(Close, pFile)
        };
        methods_.xRead = [](sqlite3_file *pFile, void *zBuf, int iAmt, sqlite3_int64 iOfst) {
            __SQLITEVFS_FILE_DISPATCH(Read, pFile, zBuf, iAmt, iOfst);
        };
        methods_.xWrite = [](sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite3_int64 iOfst) {
            __SQLITEVFS_FILE_DISPATCH(Write, pFile, zBuf, iAmt, iOfst);
        };
        methods_.xTruncate = [](sqlite3_file *pFile, sqlite3_int64 size) {
            __SQLITEVFS_FILE_DISPATCH(Truncate, pFile, size);
        };
        methods_.xSync = [](sqlite3_file *pFile, int flags) {
            __SQLITEVFS_FILE_DISPATCH(Sync, pFile, flags);
        };
        methods_.xFileSize = [](sqlite3_file *pFile, sqlite3_int64 *pSize) {
            __SQLITEVFS_FILE_DISPATCH(FileSize, pFile, pSize);
        };
        methods_.xLock = [](sqlite3_file *pFile, int eLock) {
            __SQLITEVFS_FILE_DISPATCH(Lock, pFile, eLock);
        };
        methods_.xUnlock = [](sqlite3_file *pFile, int eLock) {
            __SQLITEVFS_FILE_DISPATCH(Unlock, pFile, eLock);
        };
        methods_.xCheckReservedLock = [](sqlite3_file *pFile, int *pResOut) {
            __SQLITEVFS_FILE_DISPATCH(CheckReservedLock, pFile, pResOut);
        };
        methods_.xFileControl = [](sqlite3_file *pFile, int op, void *pArg) {
            __SQLITEVFS_FILE_DISPATCH(FileControl, pFile, op, pArg);
        };
        methods_.xSectorSize = [](sqlite3_file *pFile) {
            __SQLITEVFS_FILE_DISPATCH(SectorSize, pFile);
        };
        methods_.xDeviceCharacteristics = [](sqlite3_file *pFile) {
            __SQLITEVFS_FILE_DISPATCH(DeviceCharacteristics, pFile);
        };
        if (methods_.iVersion >= 2) {
            methods_.xShmMap = [](sqlite3_file *pFile, int iPg, int pgsz, int isWrite,
                                  void volatile **pp) {
                __SQLITEVFS_FILE_DISPATCH(ShmMap, pFile, iPg, pgsz, isWrite, pp);
            };
            methods_.xShmLock = [](sqlite3_file *pFile, int offset, int n, int flags) {
                __SQLITEVFS_FILE_DISPATCH(ShmLock, pFile, offset, n, flags);
            };
            methods_.xShmBarrier = [](sqlite3_file *pFile) {
                try {
                    reinterpret_cast<Handle *>(pFile)->this_->ShmBarrier();
                } catch (...) {
                }
            };
            methods_.xShmUnmap = [](sqlite3_file *pFile, int deleteFlag) {
                __SQLITEVFS_FILE_DISPATCH(ShmUnmap, pFile, deleteFlag);
            };
        }
        if (methods_.iVersion >= 3) {
            methods_.xFetch = [](sqlite3_file *pFile, sqlite3_int64 iOfst, int iAmt, void **pp) {
                __SQLITEVFS_FILE_DISPATCH(Fetch, pFile, iOfst, iAmt, pp);
            };
            methods_.xUnfetch = [](sqlite3_file *pFile, sqlite3_int64 iOfst, void *p) {
                __SQLITEVFS_FILE_DISPATCH(Unfetch, pFile, iOfst, p);
            };
        }
#undef __SQLITEVFS_FILE_DISPATCH
        handle->this_ = this;
        handle->base.pMethods = &methods_;
    }
};

/**
 * Implements File by wrapping some other sqlite3_file*. This accomplishes nothing useful itself,
 * but lets subclasses override individual methods.
 */
class FileWrapper : public File {
  protected:
    std::shared_ptr<sqlite3_file> wrapped_;

    int Close() override {
        int rc = wrapped_->pMethods ? wrapped_->pMethods->xClose(wrapped_.get()) : SQLITE_OK;
        wrapped_.reset(); // deletes this!
        int rc2 = File::Close();
        return rc != SQLITE_OK ? rc : rc2;
    }
    int Read(void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        return wrapped_->pMethods->xRead(wrapped_.get(), zBuf, iAmt, iOfst);
    }
    int Write(const void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        return wrapped_->pMethods->xWrite(wrapped_.get(), zBuf, iAmt, iOfst);
    }
    int Truncate(sqlite3_int64 size) override {
        return wrapped_->pMethods->xTruncate(wrapped_.get(), size);
    }
    int Sync(int flags) override { return wrapped_->pMethods->xSync(wrapped_.get(), flags); }
    int FileSize(sqlite3_int64 *pSize) override {
        return wrapped_->pMethods->xFileSize(wrapped_.get(), pSize);
    }
    int Lock(int eLock) override { return wrapped_->pMethods->xLock(wrapped_.get(), eLock); }
    int Unlock(int eLock) override { return wrapped_->pMethods->xUnlock(wrapped_.get(), eLock); }
    int CheckReservedLock(int *pResOut) override {
        return wrapped_->pMethods->xCheckReservedLock(wrapped_.get(), pResOut);
    }
    int FileControl(int op, void *pArg) override {
        return wrapped_->pMethods->xFileControl(wrapped_.get(), op, pArg);
    }
    int SectorSize() override { return wrapped_->pMethods->xSectorSize(wrapped_.get()); }
    int DeviceCharacteristics() override {
        return wrapped_->pMethods->xDeviceCharacteristics(wrapped_.get());
    }

    int ShmMap(int iPg, int pgsz, int isWrite, void volatile **pp) override {
        return wrapped_->pMethods->xShmMap(wrapped_.get(), iPg, pgsz, isWrite, pp);
    }
    int ShmLock(int offset, int n, int flags) override {
        return wrapped_->pMethods->xShmLock(wrapped_.get(), offset, n, flags);
    }
    void ShmBarrier() override { return wrapped_->pMethods->xShmBarrier(wrapped_.get()); }
    int ShmUnmap(int deleteFlag) override {
        return wrapped_->pMethods->xShmUnmap(wrapped_.get(), deleteFlag);
    }

    int Fetch(sqlite3_int64 iOfst, int iAmt, void **pp) override {
        return wrapped_->pMethods->xFetch(wrapped_.get(), iOfst, iAmt, pp);
    }

    int Unfetch(sqlite3_int64 iOfst, void *p) override {
        return wrapped_->pMethods->xUnfetch(wrapped_.get(), iOfst, p);
    }

  public:
    // inner will be freed after xClose()
    FileWrapper(const std::shared_ptr<sqlite3_file> &inner) : wrapped_(inner) {}
    virtual ~FileWrapper() noexcept = default;

    void InitHandle(sqlite3_file *pFile) noexcept override {
        methods_.iVersion = std::min(methods_.iVersion, wrapped_->pMethods->iVersion);
        File::InitHandle(pFile);
    }
};

/**
 * Abstract base class for SQLite VFS implementations. A subclass may implement the methods, then
 * something can instantiate and call Register(vfs_name) to make it available via sqlite3_open_v2.
 */
class Base {
  protected:
    sqlite3_vfs vfs_;
    std::string name_;

    // subclass may increase if it needs a large Handle for some reason
    virtual size_t szOsFile() { return sizeof(File::Handle); }

    virtual int Open(const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags) = 0;
    virtual int Delete(const char *zPath, int syncDir) = 0;
    virtual int Access(const char *zPath, int flags, int *pResOut) = 0;
    virtual int FullPathname(const char *zName, int nPathOut, char *zPathOut) = 0;

    virtual void *DlOpen(const char *zFilename) = 0;
    virtual void DlError(int nByte, char *zErrMsg) = 0;
    virtual void (*DlSym(void *pHandle, const char *zSymbol))() = 0;
    virtual void DlClose(void *pHandle) = 0;

    virtual int Randomness(int nByte, char *zOut) = 0;
    virtual int Sleep(int nMicro) = 0;
    virtual int CurrentTime(double *pTime) = 0;
    virtual int GetLastError(int nByte, char *zErrMsg) = 0;
    virtual int CurrentTimeInt64(sqlite3_int64 *pTime) = 0;

    virtual int SetSystemCall(const char *zName, sqlite3_syscall_ptr fn) = 0;
    virtual sqlite3_syscall_ptr GetSystemCall(const char *zName) = 0;
    virtual const char *NextSystemCall(const char *zName) = 0;

  public:
    Base() {
        memset(&vfs_, 0, sizeof(vfs_));
        vfs_.iVersion = 3; // subclass may override to 2 or 1
    }

    virtual int Register(const char *name) noexcept {
        assert(vfs_.iVersion && !vfs_.zName);
        name_ = name;
        vfs_.szOsFile = szOsFile();
        vfs_.mxPathname = 1024;
        vfs_.pNext = nullptr;
        vfs_.zName = name_.c_str();
        vfs_.pAppData = this;

#define __SQLITE_VFS_DISPATCH(Method, pVfs, ...)                                                   \
    try {                                                                                          \
        return reinterpret_cast<Base *>(pVfs->pAppData)->Method(__VA_ARGS__);                      \
    } catch (std::bad_alloc &) {                                                                   \
        return SQLITE_NOMEM;                                                                       \
    } catch (...) {                                                                                \
        return SQLITE_ERROR;                                                                       \
    }
        vfs_.xOpen = [](sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags,
                        int *pOutFlags) noexcept {
            __SQLITE_VFS_DISPATCH(Open, pVfs, zName, pFile, flags, pOutFlags)
        };
        vfs_.xDelete = [](sqlite3_vfs *pVfs, const char *zPath, int syncDir) noexcept {
            __SQLITE_VFS_DISPATCH(Delete, pVfs, zPath, syncDir)
        };
        vfs_.xAccess = [](sqlite3_vfs *pVfs, const char *zPath, int flags, int *pResOut) noexcept {
            __SQLITE_VFS_DISPATCH(Access, pVfs, zPath, flags, pResOut)
        };
        vfs_.xFullPathname = [](sqlite3_vfs *pVfs, const char *zName, int nPathOut,
                                char *zPathOut) noexcept {
            __SQLITE_VFS_DISPATCH(FullPathname, pVfs, zName, nPathOut, zPathOut)
        };
        vfs_.xDlOpen = [](sqlite3_vfs *pVfs, const char *zFilename) noexcept {
            try {
                return reinterpret_cast<Base *>(pVfs->pAppData)->DlOpen(zFilename);
            } catch (...) {
                return (void *)nullptr;
            }
        };
        vfs_.xDlError = [](sqlite3_vfs *pVfs, int nByte, char *zErrMsg) noexcept {
            try {
                return reinterpret_cast<Base *>(pVfs->pAppData)->DlError(nByte, zErrMsg);
            } catch (...) {
            }
        };
        vfs_.xDlSym = [](sqlite3_vfs *pVfs, void *pHandle, const char *zSymbol) noexcept {
            try {
                return reinterpret_cast<Base *>(pVfs->pAppData)->DlSym(pHandle, zSymbol);
            } catch (...) {
                return (void (*)()) nullptr;
            }
        };
        vfs_.xDlClose = [](sqlite3_vfs *pVfs, void *pHandle) noexcept {
            try {
                return reinterpret_cast<Base *>(pVfs->pAppData)->DlClose(pHandle);
            } catch (...) {
            }
        };
        vfs_.xRandomness = [](sqlite3_vfs *pVfs, int nByte, char *zOut) noexcept {
            __SQLITE_VFS_DISPATCH(Randomness, pVfs, nByte, zOut)
        };
        vfs_.xSleep = [](sqlite3_vfs *pVfs, int nMicro) noexcept {
            __SQLITE_VFS_DISPATCH(Sleep, pVfs, nMicro)
        };
        vfs_.xCurrentTime = [](sqlite3_vfs *pVfs, double *pTime) noexcept {
            __SQLITE_VFS_DISPATCH(CurrentTime, pVfs, pTime)
        };
        vfs_.xGetLastError = [](sqlite3_vfs *pVfs, int nByte, char *zErrMsg) noexcept {
            __SQLITE_VFS_DISPATCH(GetLastError, pVfs, nByte, zErrMsg)
        };
        if (vfs_.iVersion >= 2) {
            vfs_.xCurrentTimeInt64 = [](sqlite3_vfs *pVfs, sqlite3_int64 *pTime) noexcept {
                __SQLITE_VFS_DISPATCH(CurrentTimeInt64, pVfs, pTime)
            };
        }
        if (vfs_.iVersion >= 3) {
            vfs_.xSetSystemCall = [](sqlite3_vfs *pVfs, const char *zName,
                                     sqlite3_syscall_ptr fn) noexcept {
                __SQLITE_VFS_DISPATCH(SetSystemCall, pVfs, zName, fn);
            };
            vfs_.xGetSystemCall = [](sqlite3_vfs *pVfs, const char *zName) noexcept {
                try {
                    return reinterpret_cast<Base *>(pVfs->pAppData)->GetSystemCall(zName);
                } catch (...) {
                    return (sqlite3_syscall_ptr) nullptr;
                }
            };
            vfs_.xNextSystemCall = [](sqlite3_vfs *pVfs, const char *zName) noexcept {
                try {
                    return reinterpret_cast<Base *>(pVfs->pAppData)->NextSystemCall(zName);
                } catch (...) {
                    return (const char *)nullptr;
                }
            };
        }
#undef __SQLITE_VFS_DISPATCH
        return sqlite3_vfs_register(&vfs_, 0);
    }
};

/**
 * Implements VFS by wrapping some other existing VFS (perhaps the default one). This accomplishes
 * nothing useful itself, but lets subclasses override individual methods.
 */
class Wrapper : public Base {
  protected:
    sqlite3_vfs *wrapped_ = nullptr;

    // instantiate FileWrapper; subclasses may override to instantiate an associated subclass of
    // FileWrapper. zName may be used to access sqlite_uri_*.
    virtual std::unique_ptr<FileWrapper>
    NewFileWrapper(const char *zName, int flags,
                   const std::shared_ptr<sqlite3_file> &wrapped_file) {
        return std::unique_ptr<FileWrapper>(new FileWrapper(wrapped_file));
    }

    // xOpen a FileWrapper
    int Open(const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags) override {
        assert(!pFile->pMethods);
        std::shared_ptr<sqlite3_file> wrapped_file(
            (sqlite3_file *)sqlite3_malloc(wrapped_->szOsFile), sqlite3_free);
        if (!wrapped_file) {
            return SQLITE_NOMEM;
        }
        memset(wrapped_file.get(), 0, wrapped_->szOsFile);

        auto fw = NewFileWrapper(zName, flags, wrapped_file);
        fw->InitHandle(pFile);
        assert(pFile->pMethods);
        // Since pFile->pMethods is now set, caller is required to invoke xClose(), which in turn
        // closes+frees the wrapped_file and deletes the FileWrapper.
        fw.release();
        return wrapped_->xOpen(wrapped_, zName, wrapped_file.get(), flags, pOutFlags);
    }
    int Delete(const char *zPath, int syncDir) override {
        return wrapped_->xDelete(wrapped_, zPath, syncDir);
    }
    int Access(const char *zPath, int flags, int *pResOut) override {
        return wrapped_->xAccess(wrapped_, zPath, flags, pResOut);
    }
    int FullPathname(const char *zName, int nPathOut, char *zPathOut) override {
        return wrapped_->xFullPathname(wrapped_, zName, nPathOut, zPathOut);
    }

    void *DlOpen(const char *zFilename) override { return wrapped_->xDlOpen(wrapped_, zFilename); }
    void DlError(int nByte, char *zErrMsg) override {
        wrapped_->xDlError(wrapped_, nByte, zErrMsg);
    }
    void (*DlSym(void *pHandle, const char *zSymbol))() override {
        return wrapped_->xDlSym(wrapped_, pHandle, zSymbol);
    }
    void DlClose(void *pHandle) override { wrapped_->xDlClose(wrapped_, pHandle); }

    int Randomness(int nByte, char *zOut) override {
        return wrapped_->xRandomness(wrapped_, nByte, zOut);
    }
    int Sleep(int nMicro) override { return wrapped_->xSleep(wrapped_, nMicro); }
    int CurrentTime(double *pTime) override { return wrapped_->xCurrentTime(wrapped_, pTime); }
    int GetLastError(int nByte, char *zErrMsg) override {
        return wrapped_->xGetLastError(wrapped_, nByte, zErrMsg);
    }
    int CurrentTimeInt64(sqlite3_int64 *pTime) override {
        return wrapped_->xCurrentTimeInt64(wrapped_, pTime);
    }

    int SetSystemCall(const char *zName, sqlite3_syscall_ptr fn) override {
        return wrapped_->xSetSystemCall(wrapped_, zName, fn);
    }
    sqlite3_syscall_ptr GetSystemCall(const char *zName) override {
        return wrapped_->xGetSystemCall(wrapped_, zName);
    }
    const char *NextSystemCall(const char *zName) override {
        return wrapped_->xNextSystemCall(wrapped_, zName);
    }

  public:
    int Register(const char *name, const char *wrapped_vfs_name) noexcept {
        wrapped_ = sqlite3_vfs_find(wrapped_vfs_name);
        vfs_.iVersion = std::min(vfs_.iVersion, wrapped_->iVersion);
        return Base::Register(name);
    }

    int Register(const char *name) noexcept override { return Register(name, nullptr); }
};

} // namespace SQLiteVFS
