// Copyright (C) 2011  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

#include <algorithm>
#include <getopt.h>
#include <QDebug>
#include <QStringList>
#include <QHash>
#include "options.h"

using namespace Acoustid;

Option::Option(const QString &longName)
	: m_longName(longName), m_shortName(0), m_argument(NoArgument)
{
	m_metaVar = m_longName.toUpper();
}

Option &Option::setShortName(char shortName)
{
	m_shortName = shortName;
	return *this;
}

Option &Option::setArgument(ArgumentType type)
{
	m_argument = type;
	return *this;
}

Option &Option::setDefaultValue(const QString &def)
{
	m_default = def;
	return *this;
}

Option &Option::setHelp(const QString &help)
{
	m_help = help;
	return *this;
}

Option &Option::setMetaVar(const QString &metaVar)
{
	m_metaVar = metaVar;
	return *this;
}

OptionParser::OptionParser(const QString &usage)
	: m_usage(usage)
{
	addOption("help").setShortName('h').setHelp("show this help message and exit");
}

OptionParser::~OptionParser()
{
	qDeleteAll(m_options);
}

Option &OptionParser::addOption(const QString &longName, char shortName)
{
	Option *option = new Option(longName);
	if (shortName) {
		option->setShortName(shortName);
	}
	m_options.append(option);
	return *option;
}

QString OptionParser::generateHelp()
{
	QStringList help;
	help << "Options:";
	QStringList opts;
	int width = 0;
	for (int i = 0; i < m_options.size(); i++) {
		const Option *option = m_options.at(i);
		QString opt;
		if (option->argument() == Option::NoArgument) {
			if (option->shortName()) {
				opt += QString("-%1, ").arg(option->shortName());
			}
			opt += QString("--%1").arg(option->longName());
		}
		else {
			if (option->shortName()) {
				opt += QString("-%1 %2, ").arg(option->shortName()).arg(option->metaVar());
			}
			opt += QString("--%1=%2").arg(option->longName()).arg(option->metaVar());
		}
		if (opt.size() > width) {
			width = opt.size();
		}
		opts.append(opt);
	}
	for (int i = 0; i < m_options.size(); i++) {
		const Option *option = m_options.at(i);
		help.append(QString("  %1  %2").arg(opts.at(i), -width).arg(option->help()));
	}
	return help.join("\n");
}

QString OptionParser::generateUsage()
{
	return QString("Usage: ") + m_usage.replace("%prog", m_prog);
}

void OptionParser::showHelp()
{
	QString usage = generateUsage();
	QString help = generateHelp();
	fprintf(stderr, "%s\n\n%s\n", qPrintable(usage), qPrintable(help));
}

void OptionParser::exit(int code)
{
	::exit(code);
}

void OptionParser::error(const QString &message)
{
	QString usage = generateUsage();
	fprintf(stderr, "%s\n\n%s\n", qPrintable(usage), qPrintable(message));
	exit(EXIT_FAILURE);
}

Options *OptionParser::parse(int argc, char *const argv[])
{
	Options *options = new Options();
	QByteArray shortOptions;
	int shortIndexes[256];
	std::fill(shortIndexes, shortIndexes + 256, -1);
	struct option *longOptions = new struct option[m_options.size() + 1];
	int i;
	for (i = 0; i < m_options.size(); i++) {
		const Option *option = m_options.at(i);
		QByteArray name = option->longName().toLocal8Bit();
		longOptions[i].name = strdup(name.constData());
		if (option->argument() == Option::NoArgument) {
			longOptions[i].has_arg = no_argument;
		}
		else {
			longOptions[i].has_arg = required_argument;
		}
		longOptions[i].flag = 0;
		longOptions[i].val = 0;
		if (option->shortName()) {
			shortOptions += option->shortName();
			if (option->argument() != Option::NoArgument) {
				shortOptions += ':';
			}
			shortIndexes[option->shortName()] = i;
		}
		if (!option->defaultValue().isNull()) {
			options->addOption(name, option->defaultValue());
		}
	}
	longOptions[i].name = 0;
	longOptions[i].has_arg = 0;
	longOptions[i].flag = 0;
	longOptions[i].val = 0;

	m_prog = argv[0];
	int longIndex = 0;
	while (true) {
		int c = getopt_long(argc, argv, shortOptions.constData(), longOptions, &longIndex);
		if (c == -1) {
			break;
		}
		if (c == 0 || shortIndexes[c] != -1) {
			i = (c == 0) ? longIndex : shortIndexes[c];
			QString name = m_options[i]->longName();
			options->addOption(name, QString::fromLocal8Bit(optarg));
			if (name == "help") {
				showHelp();
				exit(EXIT_SUCCESS);
			}
		}
		else {
			showHelp();
			exit(EXIT_FAILURE);
		}
	}
	while (optind < argc) {
		options->addArgument(QString::fromLocal8Bit(argv[optind++]));
	}
	for (i = 0; i < m_options.size(); i++) {
		free((void *) longOptions[i].name);
	}
	delete[] longOptions;
	return options;
}

