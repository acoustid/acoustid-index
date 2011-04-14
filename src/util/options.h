// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#ifndef ACOUSTID_UTIL_OPTIONS_H_
#define ACOUSTID_UTIL_OPTIONS_H_

#include <QString>
#include <QList>
#include <QHash>

namespace Acoustid {

class Option
{
public:
	enum ArgumentType {
		NoArgument,
		StringArgument,
	};

	Option(const QString &longName);
	Option &setShortName(char shortName);
	Option &setArgument(ArgumentType type = StringArgument);
	Option &setHelp(const QString &help);
	Option &setMetaVar(const QString &metaVar);

	char shortName() const { return m_shortName; };
	const QString &longName() const { return m_longName; };
	const QString &metaVar() const { return m_metaVar; };
	const QString &help() const { return m_help; };
	ArgumentType argument() const { return m_argument; };

private:
	QString m_longName;
	char m_shortName;
	ArgumentType m_argument;
	QString m_help;
	QString m_metaVar;
};

class Options
{
public:
	Options()
	{
	}

	Options(const QHash<QString, QString> &options, const QList<QString> &arguments)
		: m_options(options), m_arguments(arguments)
	{
	}

	bool contains(const QString &name) const
	{
		return m_options.contains(name);
	}

	QString option(const QString &name) const
	{
		return m_options.value(name);
	}

	int argumentCount() const
	{
		return m_arguments.size();
	}

	const QString &argument(int i) const
	{
		return m_arguments.at(i);
	}

	const QList<QString> &arguments() const
	{
		return m_arguments;
	}

	void addArgument(const QString &argument)
	{
		m_arguments.append(argument);
	}

	void addOption(const QString &name, const QString &value)
	{
		m_options.insert(name, value);
	}

private:
	QHash<QString, QString> m_options;
	QList<QString> m_arguments;
};

/*
	OptionParser parser("%prog [options]");
	parser.addOption("file", 'f').setArgument().setHelp("input file").setMetaVar("INPUT");
	parser.addOption("test", 't');
	parser.parse(argc, argv);
*/

class OptionParser
{
public:
	OptionParser(const QString &usage = QString("%prog [options]"));
	~OptionParser();
	Option &addOption(const QString &longName, char shortName = 0);
	Options *parse(int argc, char *const argv[]);
	virtual void error(const QString &message);
	virtual void exit(int code);
private:
	QString generateHelp();
	QString generateUsage();
	void showHelp();
	QList<Option *> m_options;
	QString m_usage;
	QString m_prog;
};

}

#endif
