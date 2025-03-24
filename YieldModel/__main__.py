#!/usr/bin/env python3

"""
Machine learning model for crop yield prediction.

Written by Adam J. Stewart, 2018.
"""

from model.metrics import *
from model.regressor import *
from utils.data_tools import *
from utils.io_tools import *

import argparse
import collections
import colorama
import os
import sys

class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter,
                          argparse.ArgumentDefaultsHelpFormatter):
    """Custom argparse help formatter.

    Allows for newline characters in description.
    Automatically adds argument defaults.
    """


def set_up_parser():
    """Sets up the argument parser.

    Returns:
        argparse.ArgumentParser: the argument parser
    """
    # Find the root directory of the project
    ROOT_DIRECTORY = os.path.realpath(sys.path[0])

    # Initialize new parser
    parser = argparse.ArgumentParser(
        prog='crop_yield_prediction',
        description=__doc__,
        formatter_class=CustomHelpFormatter)

    # I/O arguments
    parser.add_argument(
        '-i', '--input-file',
        default=os.path.join(ROOT_DIRECTORY, 'data', 'wheat_model_dataset_1900_2024.csv'),
        help='input dataset for yield prediction')
    parser.add_argument(
        '-o', '--output-dir',
        default=os.path.join(ROOT_DIRECTORY, 'results'),
        help='directory to save results in')

    # Model and cross-validation scheme
    parser.add_argument(
        '-m', '--model',
        default='linear',
        choices=['linear', 'ridge', 'lasso', 'svr', 'random-forest', 'mlp'],
        help='regression model to use')
    parser.add_argument(
        '-c', '--cross-validation',
        default='leave-one-out',
        choices=['leave-one-out', 'forward'],
        help='cross-validation technique to perform')

    # Training and testing window
    parser.add_argument(
        '--start-train-year',
        default=1980, type=int,
        help='year to start training from')
    parser.add_argument(
        '--end-train-year',
        default=2014, type=int,
        help='year to end training with')
    parser.add_argument(
        '--start-test-year',
        default=2015, type=int,
        help='year to start testing from')
    parser.add_argument(
        '--end-test-year',
        default=2020, type=int,
        help='year to end testing with')

    # Hyperparameters
    parser.add_argument(
        '--ridge-lasso-alpha',
        default=1.0, type=float,
        help='regularization strength')

    parser.add_argument(
        '--svr-kernel',
        default='rbf',
        choices=['linear', 'poly', 'rbf', 'sigmoid'],
        help='SVR kernel type')
    parser.add_argument(
        '--svr-gamma',
        default=0.001, type=float,
        help='SVR kernel coefficient')
    parser.add_argument(
        '--svr-c',
        default=1.0, type=float,
        help='SVR penalty parameter C of the error term')
    parser.add_argument(
        '--svr-epsilon',
        default=0.1, type=float,
        help='epsilon in the epsilon-SVR model')

    # Utility flags
    parser.add_argument(
        '-j', '--jobs',
        default=-1, type=int,
        help='number of jobs to run in parallel')
    parser.add_argument(
        '--no-color',
        action='store_true',
        help='disable colored output')
    parser.add_argument(
        '-v', '--verbose',
        default=3, type=int,
        help='verbosity level')

    # Verbosity levels:
    #   0: print nothing
    #   1: print year
    #   2: print testing, debugging
    #   3: print training

    return parser


def main(args):
    """High-level pipeline.

    Trains the model and performs cross-validation.

    Parameters:
        args (argparse.Namespace): command-line arguments
    """
    # Read in the dataset
    input_data = read_dataset(args.input_file, args.verbose)

    output_data = input_data.copy()

    # Initialize a new regression model
    model = get_regressor(
        args.model, args.ridge_lasso_alpha,
        args.svr_kernel, args.svr_gamma, args.svr_c, args.svr_epsilon,
        args.verbose, args.jobs)

    yearly_stats = collections.defaultdict(dict)

    # For each year...
    for year in range(args.start_test_year, args.end_test_year + 1):
        if args.verbose > 0:
            print(colorama.Fore.GREEN + '\nYear:', year)

        # Split the dataset into training and testing data
        train_data, test_data = split_dataset(
            input_data, args.start_train_year, args.end_train_year,
            year, args.cross_validation)

        # Remove the annual trend
        train_data, train_years, test_data, test_years, annual_model = \
            remove_annual_trend(train_data, test_data, args.jobs)

        # Remove the county fixed effect
        train_data, train_dep, test_data, test_dep, county_fixed_effect = \
            remove_county_fixed_effect(train_data, test_data)
        
        # Remove data that we don't want to train on
        drop_cols(train_data)
        drop_cols(test_data)
        drop_nans(train_data)
        drop_nans(test_data)

        train_years = train_data.pop('year')
        test_years = test_data.pop('year')
        train_dep = train_data.pop('DEP')
        test_dep = test_data.pop('DEP')

        # Shuffle the training data
        train_data = shuffle(train_data)

        # Split the data into features and labels
        train_X, train_y = train_data, train_data.pop('yield')
        test_X, test_y = test_data, test_data.pop('yield')

        # Standardize the training and testing features
        train_X, test_X = standardize(train_X, test_X)

        # Train the model
        if args.verbose > 1:
            print(colorama.Fore.BLUE + '\nTraining...\n')

        model.fit(train_X, train_y)

        predictions = model.predict(train_X)

        train_y, predictions = reapply_county_fixed_effect(
            train_y, predictions, train_dep, county_fixed_effect, year)
        
        train_y, predictions = reapply_annual_trend(
            train_y, predictions, train_years, annual_model, year)

        # Evaluate the performance
        yearly_stats[year]['train'] = \
            calculate_statistics(train_y, predictions)

        if args.verbose > 2:
            print_statistics(yearly_stats[year]['train'])

        # Test the model
        if args.verbose > 1:
            print(colorama.Fore.BLUE + '\nTesting...\n')

        predictions = model.predict(test_X)

        test_y, predictions = reapply_county_fixed_effect(
            test_y, predictions, test_dep, county_fixed_effect, year)
        test_y, predictions = reapply_annual_trend(
            test_y, predictions, test_years, annual_model, year)
        
        # Evaluate the performance
        yearly_stats[year]['test'] = calculate_statistics(test_y, predictions)

        if args.verbose > 1:
            print_statistics(yearly_stats[year]['test'])

        save_predictions(output_data, predictions, year)

    # Evaluate the overall performance
    #drop_cols(output_data)
    drop_nans(output_data)

    labels = output_data['yield']
    predictions = output_data['predicted yield']

    overall_stats = calculate_overall_statistics(yearly_stats)
    overall_stats['test']['combined'] = \
        calculate_statistics(labels, predictions)

    print_overall_statistics(overall_stats, args.verbose)

    # Write the resulting dataset
    # write_dataset(output_data, args.output_dir, args.model, args.verbose)

    write_performance(args.output_dir, args.model, args.ridge_lasso_alpha,
                      args.svr_kernel, args.svr_gamma, args.svr_c,
                      args.svr_epsilon, overall_stats, args.verbose)
    
    output_data['predicted production'] = output_data['predicted yield'] * output_data['area']
    
    plot_yields(output_data, args.output_dir, args.model)

    current_data = pd.read_csv(os.path.join(os.path.realpath(sys.path[0]), 'data', 'wheat_model_current.csv'),)
    current_data = current_data.drop("Unnamed: 0", axis=1)
    current_year = current_data.pop("year")
    current_deps = current_data.pop("DEP")
    current_surf = current_data.pop("CULT_SURF")

    current_X = standardize_current(current_data)

    predictions = model.predict(current_X)

    predictions = reapply_current_county_fixed_effect(
            predictions, current_deps, county_fixed_effect)
    
    predictions = reapply_current_annual_trend(predictions, current_year, annual_model)

    predictions = predictions.rename_axis('DEP').reset_index(name='predicted yield')
    output_current = pd.DataFrame(predictions)
    output_current['year'] = current_year   
    output_current['area'] = current_surf

    output_current['predicted current production'] = output_current['predicted yield'] * output_current['area']

    plot_current_yields(output_data, args.output_dir, output_current, args.model)
    plot_production(output_data, args.output_dir, args.model)
    plot_current_production(output_data, args.output_dir, output_current, args.model)

if __name__ == '__main__':
    # Parse supplied arguments
    parser = set_up_parser()
    args = parser.parse_args()

    if args.verbose:
        colorama.init(autoreset=True, strip=args.no_color)

    main(args)
