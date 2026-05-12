import Layout from '@theme/Layout';
import {useColorMode} from '@docusaurus/theme-common';
import {JSX} from 'react';
import Button from '../components/Button';
import {
    Activity,
    BarChart2,
    Box,
    Code,
    Database,
    Layers,
    Puzzle,
    Settings,
    Zap,
    Radio,
} from 'lucide-react';

const Capabilities = () => {
    const capabilities = [

        {
            title: 'Time-Series Query Language',
            description:
                'Express signal arithmetic, event conditions, and aggregations in TSAL — ' +
                'a concise, Matlab-style Python syntax.',
            icon: Code,
        },
        {
            title: 'Pluggable Query Engine',
            description:
                'Compile TSAL expressions into distributed Spark execution via interchangeable solvers ' +
                'tuned to each silver-layer layout.',
            icon: Puzzle,
        },
        {
            title: 'Domain-Specific Data Model',
            description:
                'Measurement recordings modeled as containers of channels, each enriched with ' +
                'container- and channel-level attributes and metrics.',
            icon: Layers,
        },
        {
            title: 'Domain-Aware Aggregations',
            description:
                'Compute histograms, 2D heatmaps, and event-scoped statistics, ' +
                'weighted by duration, distance, or a custom expression.',
            icon: BarChart2,
        },
        {
            title: 'Event Detection',
            description:
                'Define events from boolean signal logic and extract event instances with start/end timestamps.',
            icon: Activity,
        },
        {
            title: 'Channel Scalability',
            description:
                'Supports and scales to thousands of channels with different sampling rates, ' +
                'handling diverse sensor data simultaneously.',
            icon: Radio,
        },
        {
            title: 'PySpark Native',
            description:
                'Built on Apache Spark and Delta Lake for distributed processing of petabyte-scale sensor data.',
            icon: Zap,
        },
        {
            title: 'Star Schema Output',
            description:
                'Persist results to a normalized gold layer with dimension and fact tables.',
            icon: Database,
        },
        {
            title: 'Unity Catalog Integration',
            description:
                'Keep outputs governed and discoverable in enterprise Databricks lakehouse environments.',
            icon: Box,
        },
        {
            title: 'Config-Driven Setup',
            description:
                'Control source tables, sink targets, and dimensions from JSON configuration files.',
            icon: Settings,
        },
    ];

    return (
        <div className="mt-20 mb-6 px-10">
            <h2 className="text-3xl md:text-4xl font-semibold text-center mb-6">
                Capabilities
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 w-full">
                {capabilities.map((capability, index) => {
                    const Icon = capability.icon;
                    return (
                        <div
                            key={index}
                            className="bg-white dark:bg-gray-800 shadow-lg rounded-lg p-4 text-center border border-gray-200 dark:border-gray-700 hover:shadow-xl transition-shadow"
                        >
                            <Icon className="w-7 h-7 mx-auto mb-2 text-blue-500"/>
                            <h3 className="text-base font-semibold mb-2 text-gray-800 dark:text-gray-100">
                                {capability.title}
                            </h3>
                            <p className="text-gray-600 dark:text-gray-400 text-sm">
                                {capability.description}
                            </p>
                        </div>
                    );
                })}
            </div>
        </div>
    );
};

const Hero = () => {
    const {colorMode} = useColorMode();
    const logoSrc = colorMode === 'dark' ? 'img/impulse_logo_labs_dark.svg' : 'img/impulse_logo_labs.svg';

    return (
        <div className="px-4 md:px-10 pt-10 flex flex-col items-center w-full">
            <div className="m-2">
                <img src={logoSrc} alt="Impulse Logo" className="w-[36rem] md:w-[48rem]"/>
            </div>
            <p className="text-center text-gray-600 dark:text-gray-400 mb-4">
                Provided by <a href="https://github.com/databrickslabs"
                               className="underline text-blue-500 hover:text-blue-700">Databricks Labs</a>
            </p>
            <p className="text-lg text-center">
                Impulse is a Python-based analytics library designed for<br/>
                processing large-scale time-series measurement data.
            </p>


            <div className="mt-12 grid grid-cols-1 md:grid-cols-3 gap-4">
                <Button
                    variant="secondary"
                    outline={true}
                    link="/docs/motivation"
                    size="large"
                    label="Motivation"
                    className="w-full"
                />
                <Button
                    variant="secondary"
                    outline={true}
                    link="/docs/getting_started"
                    size="large"
                    label="Get Started"
                    className="w-full"
                />
                <Button
                    variant="secondary"
                    outline={true}
                    link="/docs/tutorial/demo"
                    size="large"
                    label="Demo"
                    className="w-full"
                />
            </div>
        </div>
    );
};

export default function Home(): JSX.Element {
    return (
        <Layout>
            <main>
                <div className="flex justify-center mx-auto">
                    <div className="max-w-screen-lg">
                        <Hero/>
                        <Capabilities/>
                    </div>
                </div>
            </main>
        </Layout>
    );
}
