// Copyright (C) 2021  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
#define ACOUSTID_INDEX_IN_MEMORY_INDEX_H_

#include <QHash>
#include <QMultiHash>
#include <QReadWriteLock>
#include <QString>
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "base_index.h"

namespace Acoustid {

struct InMemoryIndexData;

class InMemoryIndexDoc {
 public:
    InMemoryIndexDoc(uint32_t id, bool isDeleted) : m_id(id), m_isDeleted(isDeleted) {}

    uint32_t id() const { return m_id; }
    bool isDeleted() const { return m_isDeleted; }

 private:
    uint32_t m_id;
    bool m_isDeleted;
};

class InMemoryIndexDocsIterator {
 public:
    InMemoryIndexDocsIterator(std::map<uint32_t, bool>::const_iterator it) : m_it(it) {}

    InMemoryIndexDoc operator*() const { return InMemoryIndexDoc(m_it->first, m_it->second); }

    uint32_t id() const { return m_it->first; }
    bool isDeleted() const { return m_it->second; }

    bool operator==(const InMemoryIndexDocsIterator &other) const { return m_it == other.m_it; }
    bool operator!=(const InMemoryIndexDocsIterator &other) const { return m_it != other.m_it; }

    InMemoryIndexDocsIterator &operator++() {
        ++m_it;
        return *this;
    }
    InMemoryIndexDocsIterator operator++(int) {
        InMemoryIndexDocsIterator it = *this;
        ++m_it;
        return it;
    }

 private:
    std::map<uint32_t, bool>::const_iterator m_it;
};

class InMemoryIndexDocs {
 public:
    InMemoryIndexDocs() {}

    size_t size() const { return m_docs.size(); }

    void setActive(uint32_t id) { m_docs[id] = false; }
    void setDeleted(uint32_t id) { m_docs[id] = true; }

    bool get(uint32_t id, bool &isDeleted) const {
        auto it = m_docs.find(id);
        if (it == m_docs.end()) {
            return false;
        }
        isDeleted = it->second;
        return true;
    }

    void clear() { m_docs.clear(); }

    InMemoryIndexDocsIterator begin() const { return InMemoryIndexDocsIterator(m_docs.begin()); }
    InMemoryIndexDocsIterator end() const { return InMemoryIndexDocsIterator(m_docs.end()); }

 private:
    std::map<uint32_t, bool> m_docs;
};

class InMemoryIndexTerm {
 public:
    InMemoryIndexTerm(uint32_t term, uint32_t docId) : m_term(term), m_docId(docId) {}

    uint32_t term() const { return m_term; }
    uint32_t docId() const { return m_docId; }

 private:
    uint32_t m_term;
    uint32_t m_docId;
};

class InMemoryIndexTermsIterator {
 public:
    InMemoryIndexTermsIterator(std::multimap<uint32_t, uint32_t>::const_iterator it) : m_it(it) {}

    InMemoryIndexTerm operator*() const { return InMemoryIndexTerm(m_it->first, m_it->second); }

    uint32_t term() const { return m_it->first; }
    uint32_t docId() const { return m_it->second; }

    bool operator==(const InMemoryIndexTermsIterator &other) const { return m_it == other.m_it; }
    bool operator!=(const InMemoryIndexTermsIterator &other) const { return m_it != other.m_it; }

    InMemoryIndexTermsIterator &operator++() {
        ++m_it;
        return *this;
    }
    InMemoryIndexTermsIterator operator++(int) {
        InMemoryIndexTermsIterator it = *this;
        ++m_it;
        return it;
    }

 private:
    std::multimap<uint32_t, uint32_t>::const_iterator m_it;
};

class InMemoryIndexTerms {
 public:
    InMemoryIndexTerms() {}

    void insertDocument(uint32_t docId, const std::vector<uint32_t> &terms) {
        std::set<uint32_t> uniqueTerms(terms.begin(), terms.end());
        for (auto term : uniqueTerms) {
            m_terms.insert({term, docId});
        }
    }

    void deleteDocument(uint32_t docId) {
        for (auto it = m_terms.begin(); it != m_terms.end();) {
            if (it->second == docId) {
                it = m_terms.erase(it);
            } else {
                ++it;
            }
        }
    }

    std::vector<SearchResult> search(const std::vector<uint32_t> &terms) {
        std::map<uint32_t, int> hits;
        for (auto term : terms) {
            auto it = m_terms.find(term);
            while (it != m_terms.end() && it->first == term) {
                hits[it->second]++;
                ++it;
            }
        }
        std::vector<SearchResult> results;
        results.reserve(hits.size());
        for (auto it = hits.begin(); it != hits.end(); ++it) {
            results.emplace_back(it->first, it->second, 0);
        }
        sortSearchResults(results);
        return results;
    }

    void clear() { m_terms.clear(); }

    InMemoryIndexTermsIterator begin() const { return InMemoryIndexTermsIterator(m_terms.begin()); }
    InMemoryIndexTermsIterator end() const { return InMemoryIndexTermsIterator(m_terms.end()); }

 private:
    std::multimap<uint32_t, uint32_t> m_terms;
};

class InMemoryIndexSnapshot {
 public:
    InMemoryIndexSnapshot(QReadWriteLock *lock, InMemoryIndexData *data) : m_locker(lock), m_data(data) {}

    const InMemoryIndexDocs &docs() const;
    const InMemoryIndexTerms &terms() const;
    const QMap<QString, QString> &attributes() const;

 private:
    QReadLocker m_locker;
    InMemoryIndexData *m_data;
};

class InMemoryIndex : public BaseIndex {
 public:
    InMemoryIndex();
    virtual ~InMemoryIndex() override;

    // Disable copying
    InMemoryIndex(const InMemoryIndex &) = delete;
    InMemoryIndex(InMemoryIndex &&) = delete;
    InMemoryIndex &operator=(const InMemoryIndex &) = delete;
    InMemoryIndex &operator=(InMemoryIndex &&) = delete;

    uint64_t revision() const { return m_revision; }
    void setRevision(uint64_t revision) { m_revision = revision; }

    // Remove all data from the index
    void clear();

    size_t size();

    virtual bool containsDocument(uint32_t docId) override {
        bool isDeleted;
        return getDocument(docId, isDeleted) && !isDeleted;
    }

    bool getDocument(uint32_t docId, bool &isDeleted);

    virtual std::vector<SearchResult> search(const std::vector<uint32_t> &terms, int64_t timeoutInMSecs) override;

    virtual bool hasAttribute(const QString &name) override;
    virtual QString getAttribute(const QString &name) override;
    void setAttribute(const QString &name, const QString &value);

    virtual void applyUpdates(const OpBatch &batch) override;

    const OpBatch &updates();

    InMemoryIndexSnapshot snapshot();

 private:
    QReadWriteLock m_lock;
    std::atomic<uint64_t> m_revision{0};
    std::unique_ptr<InMemoryIndexData> m_data;
};

}  // namespace Acoustid

#endif  // ACOUSTID_INDEX_IN_MEMORY_INDEX_H_
