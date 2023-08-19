import React from 'react';
import classNames from 'classnames';
import { SectionTilesProps } from '../../utils/SectionProps';
import SectionHeader from './partials/SectionHeader';
// import Image from '../elements/Image';
import icon1 from '../../assets/images/feature-tile-icon-01.svg'
import icon2 from '../../assets/images/feature-tile-icon-02.svg'
import icon3 from '../../assets/images/feature-tile-icon-03.svg'



const propTypes = {
  ...SectionTilesProps.types
}

const defaultProps = {
  ...SectionTilesProps.defaults
}
const FeaturesTiles = ({
  className,
  topOuterDivider,
  bottomOuterDivider,
  topDivider,
  bottomDivider,
  hasBgColor,
  invertColor,
  pushLeft,
  ...props
}) => {

  const outerClasses = classNames(
    'features-tiles section',
    topOuterDivider && 'has-top-divider',
    bottomOuterDivider && 'has-bottom-divider',
    hasBgColor && 'has-bg-color',
    invertColor && 'invert-color',
    className
  );

  const innerClasses = classNames(
    'features-tiles-inner section-inner pt-0',
    topDivider && 'has-top-divider',
    bottomDivider && 'has-bottom-divider'
  );

  const tilesClasses = classNames(
    'tiles-wrap center-content',
    pushLeft && 'push-left'
  );

  const sectionHeader = {
    paragraph: 'C простой системой ведения и контроллинга ддс с открытым API'
  };

  return (
    <section
      {...props}
      className={outerClasses}
    >
      <div className="container">
        <div className={innerClasses}>
          <h2 className="text-center">Фин учет - это <span className="text-content-primary">просто!</span></h2>
          <SectionHeader data={sectionHeader} className="center-content" />
          
          <div className={tilesClasses}>

          <div className="tiles-item reveal-from-bottom" data-reveal-delay="200">
              <div className="tiles-item-inner">
                <div className="features-tiles-item-header">
                  <div className="features-tiles-item-image mb-16">
                      <img
                        src={icon1} 
                        width={64}
                        height={64}
                        alt="icon"
                      />
                  </div>
                </div>
                <div className="features-tiles-item-content">
                  <h4 className="mt-0 mb-8">
                  Контролируете все поступления
                    </h4>
                  <p className="m-0 text-sm">
                  Контролируете все поступления с помощью нашего приложения
                    </p>
                </div>
              </div>
            </div>

            <div className="tiles-item reveal-from-bottom" data-reveal-delay="200">
              <div className="tiles-item-inner">
                <div className="features-tiles-item-header">
                  <div className="features-tiles-item-image mb-16">
                      <img
                        src={icon2} 
                        width={64}
                        height={64}
                        alt="icon"
                      />
                  </div>
                </div>
                <div className="features-tiles-item-content">
                  <h4 className="mt-0 mb-8">
                  Заранее знаете все будущие платежи
                    </h4>
                  <p className="m-0 text-sm">
                  Заранее знаете все будущие платежи и списания с помощью нашего приложения
                    </p>
                </div>
              </div>
            </div>

            <div className="tiles-item reveal-from-bottom" data-reveal-delay="200">
              <div className="tiles-item-inner">
                <div className="features-tiles-item-header">
                  <div className="features-tiles-item-image mb-16">
                      <img
                        src={icon3} 
                        width={64}
                        height={64}
                        alt="icon"
                      />
                  </div>
                </div>
                <div className="features-tiles-item-content">
                  <h4 className="mt-0 mb-8">
                  Предотвращаете кассовые разрывы
                    </h4>
                  <p className="m-0 text-sm">
                  Предотвращаете кассовые разрывы, всего зайдите в наш телеграм.
                    </p>
                </div>
              </div>
            </div>

          </div>
        </div>
      </div>
    </section>
  );
}

FeaturesTiles.propTypes = propTypes;
FeaturesTiles.defaultProps = defaultProps;

export default FeaturesTiles;